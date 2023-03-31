package jdk.crac;

import jdk.internal.crac.LoggerContainer;
import sun.security.action.GetPropertyAction;

public enum TimerPolicy {
    /**
     * Run timers as if the process was running when the VM was in snapshot.
     * This is the default policy.
     */
    DEFAULT,
    /**
     * Cancel timers after restore without running them.
     * Application will start the timers manually during restore. This option
     * is convenient when the handle to the {@link java.util.concurrent.ScheduledFuture}
     * is not easily available.
     */
    CANCEL,
    /**
     * Time spent in snapshot is ignored, timers run as if there was no break. This involves both
     * periodic and one-time timers.
     */
    SKIP_TIME,
    /**
     * This is similar to {@link #DEFAULT} but fixed-rate periodic timers won't run
     * all the executions that were timed to the period before restore - only one execution
     * will be run instead as soon as possible.
     */
    SKIP_EXECUTIONS;

    public static final String PROPERTY = "jdk.crac.timer-policy";
    public static final TimerPolicy POLICY;

    static {
        String value = GetPropertyAction.privilegedGetProperty(PROPERTY);
        TimerPolicy policy = DEFAULT;
        if (value != null) {
            try {
                policy = TimerPolicy.valueOf(value);
            } catch (IllegalArgumentException e) {
                LoggerContainer.error("Illegal value for timer policy " + value);
            }
        }
        POLICY = policy;
    }
}
