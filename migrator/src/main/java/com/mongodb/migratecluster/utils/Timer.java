package com.mongodb.migratecluster.utils;

import java.util.TimerTask;

public class Timer extends java.util.Timer
{
    private final long delay;
    private Runnable  task;
    private TimerTask internalTimerTask;

    public Timer(long delay) {
        this.delay = delay;
    }

    public void schedule(Runnable runnable)
    {
        task = runnable;
        internalTimerTask = getInternalTimerTask();
        this.schedule(internalTimerTask, delay);
    }

    public void reset() {
        internalTimerTask.cancel();
        internalTimerTask = this.getInternalTimerTask();
        this.schedule(internalTimerTask, delay);
    }

    private TimerTask getInternalTimerTask() {
        return new TimerTask()
        {
            @Override
            public void run()
            {
                task.run();
                reset();
            }
        };
    }
}