package com.skytix.velocity.scheduler;

public enum Priority {
    IMMEDIATE, // Tasks that must be executed immediately in front of all other tasks.  Must only be used for critical operations type tasks.
    NORMAL, // Normal priority for all general tasks that are initiated by customer/front-end tasks.
    BACKGROUND // Background priority for all other tasks such as preview generation tasks and other tasks that have the least priority.
}
