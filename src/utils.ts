import { WorkflowRetryPolicy } from "./workflowTypes"

export function retryPolicy(
    initial_backoff_ms: number,
    over_ms: number, base = 2
): WorkflowRetryPolicy {
    //sum of a gp Sn = a[(rn – 1)/(r – 1)] 
    //a is the initial backoff
    //but where attempts = n = (requiredAttempts -1) or = retries
    // Sn = initial_backoff_ms = initial_backoff * ((base ^ attempts - 1) / (base - 1))
    // Sn = over_ms, so over_ms = initial_backoff * ((base ^ attempts - 1) / (base - 1))
    //over_ms * (base -1) = initial_backoff * (base ^ attempts - 1) = initial_backoff * base ^ attempts - initial_backoff
    //over_ms * (base -1) + initial_backoff = initial_backoff * base ^ attempts
    //(over_ms * (base -1) + initial_backoff)/initial_backoff = base ^ attempts
    //logbase((over_ms * (base -1) + initial_backoff)/initial_backoff) = logbase(base ^ attempts) = attempts
    //so attempts = logbase((over_ms * (base -1) + initial_backoff)/initial_backoff)
    initial_backoff_ms = initial_backoff_ms || (over_ms / 10)
    if (base <= 1) {
        const retries = over_ms / initial_backoff_ms
        return { max: Math.ceil(retries), exponent: base, initial_backoff_ms }
    }
    const retries = Math.log2((over_ms * (base - 1) + initial_backoff_ms) / initial_backoff_ms) / Math.log2(base)
    return { max: Math.ceil(retries), exponent: base, initial_backoff_ms }
}