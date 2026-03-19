/**
 * In-memory job storage implementation.
 *
 * A fully functional WorkflowJobStorage backed by @pebbletree/mvcc-testing,
 * suitable for tests and simulated job runners (no external database required).
 */
import type {
    WorkflowJobKey,
    WorkflowJobValue,
    WorkflowJobLogKey,
    WorkflowJobLogValue,
    BasicJobPayload,
} from "./workflowTypes"
import tuple from "fdb-tuple"
import type {
    atSubspaceKey,
    WorkflowJobStorage,
} from "./workflowStorageAdapter"
import { MVCCCore } from "@pebbletree/mvcc-testing"
import { TransactionFactory } from "@pebbletree/mvcc-testing/dist/types";


export class InMemoryJobStorage<PAYLOAD_T extends BasicJobPayload = BasicJobPayload>
    implements WorkflowJobStorage<PAYLOAD_T> {
    readonly JobDatabase = new MVCCCore.Store<WorkflowJobKey, WorkflowJobKey, WorkflowJobValue<PAYLOAD_T>, WorkflowJobValue<PAYLOAD_T>>({
        keyTransformer: {
            pack(value) {
                return tuple.pack([value.job_id])
            },
            unpack(buffer) {
                const [job_id] = tuple.unpack(buffer)
                if (typeof job_id !== "string") {
                    throw new Error("Invalid job_id in key")
                }
                return { job_id }
            }
        },
        prefix: "jobs"
    });
    readonly JoblogSubpace = new MVCCCore.Subspace<WorkflowJobLogKey, WorkflowJobLogKey, unknown, WorkflowJobLogValue>({
        pack(value) {
            return tuple.pack([value.timestamp, value.job_id, value.random])
        },
        unpack(buffer) {
            const [timestamp, job_id, random] = tuple.unpack(buffer)
            if (typeof job_id !== "string" || typeof timestamp !== "number" || typeof random !== "string") {
                throw new Error("Invalid log key")
            }
            return { job_id, timestamp, random }
        }
    }, "logs")
    readonly atIndex;
    readonly executorIndex = new MVCCCore.DerivedSubspace<WorkflowJobKey, WorkflowJobKey, WorkflowJobValue<PAYLOAD_T>, WorkflowJobValue<PAYLOAD_T>, { execution_id: string } & WorkflowJobKey, { execution_id: string } & WorkflowJobKey>({
        source: this.JobDatabase,
        mapKey(key, value) {
            return { execution_id: value.header.execution_id || "", job_id: key.job_id }
        },
        prefix: "executor",
        keyTransformer: {
            pack(value) {
                return tuple.pack([value.execution_id, value.job_id])
            },
            unpack(buffer) {
                const [execution_id, job_id] = tuple.unpack(buffer)
                if (typeof execution_id !== "string" || typeof job_id !== "string") {
                    throw new Error("Invalid executor index key")
                }
                return { execution_id, job_id }
            }
        }
    })
    readonly doTn: TransactionFactory<WorkflowJobKey, WorkflowJobValue<PAYLOAD_T>>;
    readonly subspaces: WorkflowJobStorage<PAYLOAD_T>["subspaces"]
    constructor(args: {
        typeIndex: boolean
    }) {
        this.doTn = this.JobDatabase.doTn.bind(this.JobDatabase);
        this.atIndex = new MVCCCore.DerivedSubspace<WorkflowJobKey, WorkflowJobKey, WorkflowJobValue<PAYLOAD_T>, WorkflowJobValue<PAYLOAD_T>, atSubspaceKey<PAYLOAD_T>, atSubspaceKey<PAYLOAD_T>>({
            source: this.JobDatabase,
            mapKey(key, value) {
                return { at: value.header.at, type: args.typeIndex ? value.payload.type : undefined, job_id: key.job_id }
            },
            prefix: "at",
            keyTransformer: {
                pack(value) {
                    if (value.type && args.typeIndex)
                        return tuple.pack([value.at, value.type, value.job_id])
                    return tuple.pack([value.at, value.job_id])
                },
                unpack(buffer) {
                    if (args.typeIndex) {
                        const [at, type, job_id] = tuple.unpack(buffer)
                        if (typeof at !== "number" || typeof job_id !== "string" || typeof type !== "string") {
                            throw new Error("Invalid at index key")
                        }
                        return { at, type, job_id }
                    } else {
                        const [at, job_id] = tuple.unpack(buffer)
                        if (typeof at !== "number" || typeof job_id !== "string") {
                            throw new Error("Invalid at index key")
                        }
                        return { at, job_id, type: undefined }
                    }
                }
            }
        })
        this.subspaces = {
            at: this.atIndex.withKeyEncoding({
                ...this.atIndex.keyXf,
                pack(value) {
                    return tuple.pack([value.at])
                },
            }),
            executor: this.executorIndex.withKeyEncoding({
                ...this.executorIndex.keyXf,
                pack(value) {
                    return tuple.pack(["executor", value.execution_id])
                },
            }),

            jobLogKey: this.JoblogSubpace
        }
    }
}
