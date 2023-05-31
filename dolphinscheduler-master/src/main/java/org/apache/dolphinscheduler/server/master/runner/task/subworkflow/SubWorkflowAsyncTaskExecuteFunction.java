/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.runner.task.subworkflow;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.repository.ProcessInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.SubProcessParameters;
import org.apache.dolphinscheduler.server.master.runner.execute.AsyncTaskExecuteFunction;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SubWorkflowAsyncTaskExecuteFunction implements AsyncTaskExecuteFunction {

    private static final Duration SUB_WORKFLOW_TASK_EXECUTE_STATE_CHECK_INTERVAL = Duration.ofSeconds(10);

    private final TaskExecutionContext taskExecutionContext;
    private final SubProcessParameters taskParameters;
    private final ProcessInstanceDao processInstanceDao;
    private ProcessInstance subWorkflowInstance;

    public SubWorkflowAsyncTaskExecuteFunction(TaskExecutionContext taskExecutionContext,
                                               SubProcessParameters taskParameters,
                                               ProcessInstanceDao processInstanceDao) {
        this.taskExecutionContext = taskExecutionContext;
        this.taskParameters = taskParameters;
        this.processInstanceDao = processInstanceDao;
    }

    @Override
    public @NonNull AsyncTaskExecutionStatus getAsyncTaskExecutionStatus() {
        // query the status of sub workflow instance
        if (subWorkflowInstance == null) {
            subWorkflowInstance = processInstanceDao.findSubProcessInstanceByParentId(
                    taskExecutionContext.getProcessInstanceId(), taskExecutionContext.getTaskInstanceId());
        }
        if (subWorkflowInstance == null) {
            log.info("The sub workflow instance doesn't created");
            return AsyncTaskExecutionStatus.RUNNING;
        }
        subWorkflowInstance = processInstanceDao.queryByWorkflowInstanceId(subWorkflowInstance.getId());
        if (subWorkflowInstance != null && subWorkflowInstance.getState().isFinished()) {
            fillOutputVariables();
            return subWorkflowInstance.getState().isSuccess() ? AsyncTaskExecutionStatus.SUCCESS
                    : AsyncTaskExecutionStatus.FAILED;
        }
        return AsyncTaskExecutionStatus.RUNNING;
    }

    @Override
    public @NonNull Duration getAsyncTaskStateCheckInterval() {
        return SUB_WORKFLOW_TASK_EXECUTE_STATE_CHECK_INTERVAL;
    }

    private void fillOutputVariables() {
        // TODO: Improve error handling logic

        List<Property> subProcessProperties = Optional.of(subWorkflowInstance) // this cannot be null
                .map(ProcessInstance::getVarPool)
                .map(subProcessVarPool -> JSONUtils.toList(subProcessVarPool, Property.class))
                .orElseGet(Collections::emptyList);

        if (subProcessProperties.isEmpty()) {
            log.warn("No subprocess properties found");
            return;
        }

        subProcessProperties.forEach(taskParameters::addPropertyToValPool);
    }
}
