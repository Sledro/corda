package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.FiberScheduler
import co.paralleluniverse.fibers.Suspendable
import co.paralleluniverse.strands.channels.Channels
import net.corda.core.concurrent.CordaFuture
import net.corda.core.flows.FlowException
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.concurrent.OpenFuture
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.internal.CheckpointSerializationContext
import net.corda.core.serialization.internal.checkpointDeserialize
import net.corda.core.utilities.contextLogger
import net.corda.node.services.api.CheckpointStorage
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.messaging.DeduplicationHandler
import net.corda.node.services.statemachine.transitions.StateMachine
import net.corda.nodeapi.internal.persistence.CordaPersistence
import org.apache.activemq.artemis.utils.ReusableLatch
import java.security.SecureRandom

class Flow(val fiber: FlowStateMachineImpl<*>, val resultFuture: OpenFuture<Any?>)

class NonResidentFlow(val runId: StateMachineRunId, val checkpoint: Checkpoint) {
    val externalEvents = mutableListOf<Event.DeliverSessionMessage>()

    fun addExternalEvent(message: Event.DeliverSessionMessage) {
        externalEvents.add(message)
    }
}

class FlowCreator(val checkpointSerializationContext: CheckpointSerializationContext,
                  private val checkpointStorage: CheckpointStorage,
                  val scheduler: FiberScheduler,
                  val database: CordaPersistence,
                  val transitionExecutor: TransitionExecutor,
                  val actionExecutor: ActionExecutor?,
                  val secureRandom: SecureRandom,
                  val serviceHub: ServiceHubInternal,
                  val unfinishedFibers: ReusableLatch,
                  val resetCustomTimeout: (StateMachineRunId, Long) -> Unit ) {

    companion object {
        private val logger = contextLogger()
    }

    fun createFlowFromNonResidentFlow(nonResidentFlow: NonResidentFlow): Flow? {
        val checkpoint = when (nonResidentFlow.checkpoint.status) {
            Checkpoint.FlowStatus.PAUSED -> {
                val serialized = database.transaction {
                    checkpointStorage.getCheckpoint(nonResidentFlow.runId)
                }
                serialized?.copy(status = Checkpoint.FlowStatus.RUNNABLE)?.deserialize(checkpointSerializationContext) ?: return null
            }
            else -> nonResidentFlow.checkpoint
        }
        return createFlowFromCheckpoint(nonResidentFlow.runId, checkpoint)
    }

    fun createFlowFromCheckpoint(runId: StateMachineRunId, oldCheckpoint: Checkpoint): Flow? {
        val checkpoint = oldCheckpoint.copy(status = Checkpoint.FlowStatus.RUNNABLE)

        val fiber = getFiberFromCheckpoint(runId, checkpoint) ?: return null
        val state = StateMachineState(
                checkpoint = checkpoint,
                pendingDeduplicationHandlers = emptyList(),
                isFlowResumed = false,
                isWaitingForFuture = false,
                isAnyCheckpointPersisted = true,
                isStartIdempotent = false,
                isRemoved = false,
                flowLogic = fiber.logic,
                senderUUID = null)
        val resultFuture = openFuture<Any?>()
        fiber.transientValues = TransientReference(createTransientValues(runId, resultFuture))
        fiber.transientState = TransientReference(state)
        fiber.logic.stateMachine = fiber
        verifyFlowLogicIsSuspendable(fiber.logic)
        return Flow(fiber, resultFuture)
    }

    private fun getFiberFromCheckpoint(runId: StateMachineRunId, checkpoint: Checkpoint): FlowStateMachineImpl<*>? {
        return when (checkpoint.flowState) {
            is FlowState.Unstarted -> {
                val logic = tryCheckpointDeserialize(checkpoint.flowState.frozenFlowLogic, runId) ?: return null
                FlowStateMachineImpl(runId, logic, scheduler)
            }
            is FlowState.Started -> {
                tryCheckpointDeserialize(checkpoint.flowState.frozenFiber, runId) ?: return null
            }
            // Places calling this function is rely on it to return null if the flow cannot be created from the checkpoint.
            else -> {
                return null
            }
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private inline fun <reified T : Any> tryCheckpointDeserialize(bytes: SerializedBytes<T>, flowId: StateMachineRunId): T? {
        return try {
            bytes.checkpointDeserialize(context = checkpointSerializationContext)
        } catch (e: Exception) {
            logger.error("Unable to deserialize checkpoint for flow $flowId. Something is very wrong and this flow will be ignored.", e)
            null
        }
    }

    private fun verifyFlowLogicIsSuspendable(logic: FlowLogic<Any?>) {
        // Quasar requires (in Java 8) that at least the call method be annotated suspendable. Unfortunately, it's
        // easy to forget to add this when creating a new flow, so we check here to give the user a better error.
        //
        // The Kotlin compiler can sometimes generate a synthetic bridge method from a single call declaration, which
        // forwards to the void method and then returns Unit. However annotations do not get copied across to this
        // bridge, so we have to do a more complex scan here.
        val call = logic.javaClass.methods.first { !it.isSynthetic && it.name == "call" && it.parameterCount == 0 }
        if (call.getAnnotation(Suspendable::class.java) == null) {
            throw FlowException("${logic.javaClass.name}.call() is not annotated as @Suspendable. Please fix this.")
        }
    }

    private fun createTransientValues(id: StateMachineRunId, resultFuture: CordaFuture<Any?>): FlowStateMachineImpl.TransientValues {
        return FlowStateMachineImpl.TransientValues(
                eventQueue = Channels.newChannel(-1, Channels.OverflowPolicy.BLOCK),
                resultFuture = resultFuture,
                database = database,
                transitionExecutor = transitionExecutor,
                actionExecutor = actionExecutor!!,
                stateMachine = StateMachine(id, secureRandom),
                serviceHub = serviceHub,
                checkpointSerializationContext = checkpointSerializationContext,
                unfinishedFibers = unfinishedFibers,
                waitTimeUpdateHook = { flowId, timeout -> resetCustomTimeout(flowId, timeout) }
        )
    }
}
