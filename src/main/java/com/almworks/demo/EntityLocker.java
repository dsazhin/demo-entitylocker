
package com.almworks.demo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @param <ID> should has immutable hashCode()
 */
@Slf4j
public class EntityLocker<ID> {

	public static final int DEFAULT_ESC_THRESHOLD = 10;

	private static final int MAX_WORKERS_COUNT = Integer.MAX_VALUE;

	private final Function<ID, EntityLock> globalLockSupplier;

	private final int globalLockThreshold;

	private final AtomicBoolean escalationFired;

	private final Semaphore globalPermits;

	private final HashMap<ID, EntityLock> entityLocks;

	private final ThreadLocal<Integer> ownedPermits;

	private final ThreadLocal<HashSet<EntityLock>> ownedLocks;

	private final ThreadLocal<Boolean> isUnderEscalation;

	private final HashMap<Thread, EntityLock> desiredLocks;

	private final Object lockStatesGuard = new Object();

	public EntityLocker(int globalLockThreshold) {
		this.globalLockThreshold = globalLockThreshold;
		this.globalPermits = new Semaphore(MAX_WORKERS_COUNT);
		this.entityLocks = new HashMap<>();
		EntityLock globalLock = new EntityLock(new ReentrantLock(), MAX_WORKERS_COUNT, lock -> {
		});
		this.globalLockSupplier = _id -> globalLock;
		this.ownedPermits = ThreadLocal.withInitial(() -> 0);
		this.ownedLocks = ThreadLocal.withInitial(HashSet::new);
		this.desiredLocks = new HashMap<>();
		this.escalationFired = new AtomicBoolean();
		this.isUnderEscalation = ThreadLocal.withInitial(() -> false);
	}

	public EntityLocker() {
		this(DEFAULT_ESC_THRESHOLD);
	}

	public void execExclusively(ID id, Runnable task) {
		log.trace("Enter execExclusively for {}", id);
		doExecProtected(id, task, globalLockSupplier);
		log.trace("Leave execExclusively");
	}

	public void execProtected(ID id, Runnable task) {
		log.trace("Enter execProtected for {}", id);
		final boolean globalLockRequired = ownedLocks.get().size() >= globalLockThreshold;
		final boolean escalationHappened = globalLockRequired && escalationFired.compareAndSet(false, true);
		if (escalationHappened) {
			isUnderEscalation.set(true);
			log.debug("Lock escalation fired");
		}
		Function<ID, EntityLock> lockSupplier;
		if (isUnderEscalation.get()) {
			lockSupplier = globalLockSupplier;
		} else {
			lockSupplier = this::getOrCreateEntityLock;
		}
		try {
			doExecProtected(id, task, lockSupplier);
		} finally {
			if (escalationHappened) {
				isUnderEscalation.remove();
				log.debug("Put lock escalation out");
			}
		}
		log.trace("Leave execProtected");
	}

	private void doExecProtected(ID id, Runnable task, Function<ID, EntityLock> entityLockSupplier) {

		try (EntityLock entityLock = entityLockSupplier.apply(id)) {
			final boolean alreadyHold = !ownedLocks.get().add(entityLock);
			try {
				final Lock innerLock = entityLock.getLock();
				final int requiredPermits = entityLock.getCost();
				final Thread currThread = Thread.currentThread();
				final Thread prevOwner;
				synchronized (lockStatesGuard) {
					desiredLocks.put(currThread, entityLock);
					entityLock.setAwaits(entityLock.getAwaits() + 1);
				}
				try {
					if (!innerLock.tryLock()) {
						// check on possible deadlock
						synchronized (lockStatesGuard) {
							for (EntityLock tryingLock = entityLock; tryingLock != null;) {
								Thread lockHolder = tryingLock.getOwner();
								if (lockHolder == currThread) {
									// possible deadlock detected, address it
									log.warn("Possible deadlock detected for key=" + id);
									throw new DeadlockDetectedException("Possible deadlock detected for key=" + id);
								} else if (lockHolder != null) {
									tryingLock = desiredLocks.get(lockHolder);
								} else {
									break;
								}
							}
						}
						innerLock.lock();
					}
					synchronized (lockStatesGuard) {
						desiredLocks.remove(currThread);
						prevOwner = entityLock.getOwner();
						entityLock.setOwner(currThread);
					}
				} finally {
					synchronized (lockStatesGuard) {
						desiredLocks.remove(currThread);
						entityLock.setAwaits(entityLock.getAwaits() - 1);
					}
				}

				try {
					final int alreadyAcquired = ownedPermits.get();
					final int remainPermits = requiredPermits - alreadyAcquired;
					if (remainPermits > 0) {
						globalPermits.acquireUninterruptibly(remainPermits);
						ownedPermits.set(requiredPermits);
					}
					try {
						task.run();
					} finally {
						if (remainPermits > 0) {
							ownedPermits.set(alreadyAcquired);
							if (alreadyAcquired == 0) {
								ownedPermits.remove();
							}
							globalPermits.release(remainPermits);
						}
					}
				} finally {
					synchronized (lockStatesGuard) {
						entityLock.setOwner(prevOwner);
					}
					innerLock.unlock();
				}
			} finally {
				if (!alreadyHold) {
					ownedLocks.get().remove(entityLock);
					if (ownedLocks.get().isEmpty()) {
						ownedLocks.remove();
					}
				}
			}
		}
	}

	private EntityLock getOrCreateEntityLock(ID id) {
		synchronized (lockStatesGuard) {
			return entityLocks.computeIfAbsent(id,
					key -> new EntityLock(new ReentrantLock(), 1, lock -> releaseEntityLock(id, lock)));
		}
	}

	private void releaseEntityLock(ID id, EntityLock lock) {
		synchronized (lockStatesGuard) {
			if (lock.getOwner() == null && lock.getAwaits() == 0) {
				entityLocks.remove(id);
			}
		}
	}

	@RequiredArgsConstructor
	@Getter
	@Setter
	private static class EntityLock implements AutoCloseable {
		private final Lock lock;

		private final int cost;

		private final Consumer<EntityLock> closeOp;

		private Thread owner;

		private int awaits;

		@Override
		public void close() {
			closeOp.accept(this);
		}
	}
}
