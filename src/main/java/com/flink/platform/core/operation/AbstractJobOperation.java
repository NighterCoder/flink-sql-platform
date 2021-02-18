package com.flink.platform.core.operation;

import com.flink.platform.core.context.SessionContext;
import com.flink.platform.core.deployment.ClusterDescriptorAdapter;
import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.core.rest.result.ColumnInfo;
import com.flink.platform.core.rest.result.ResultKind;
import com.flink.platform.core.rest.result.ResultSet;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

public abstract class AbstractJobOperation implements JobOperation {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJobOperation.class);

    protected final SessionContext context;
    // clusterDescriptorAdapter is not null only after job is submitted
    protected ClusterDescriptorAdapter<?> clusterDescriptorAdapter;
    protected final String sessionId;
    protected volatile JobID jobId;

    private long currentToken;
    private int previousMaxFetchSize;
    private int previousResultSetSize;
    private LinkedList<Row> bufferedResults;
    @Nullable
    private LinkedList<Boolean> bufferedChangeFlags;
    private boolean noMoreResults;
    private volatile boolean isJobCanceled;

    protected final Object lock = new Object();

    public AbstractJobOperation(SessionContext context) {
        this.context = context;
        this.sessionId = context.getSessionId();
        this.currentToken = 0;
        this.previousMaxFetchSize = 0;
        this.previousResultSetSize = 0;
        this.bufferedResults = new LinkedList<>();
        this.bufferedChangeFlags = null;
        this.noMoreResults = false;
        this.isJobCanceled = false;
    }

    @Override
    public JobStatus getJobStatus() {
        synchronized (lock) {
            return clusterDescriptorAdapter.getJobStatus();
        }
    }

    @Override
    public void cancelJob() {
        if (isJobCanceled) {
            // just for fast failure
            return;
        }
        synchronized (lock) {
            if (jobId == null) {
                LOG.error("Session: {}. No job has been submitted. This is a bug.", sessionId);
                throw new IllegalStateException("No job has been submitted. This is a bug.");
            }
            if (isJobCanceled) {
                return;
            }

            cancelJobInternal();
            isJobCanceled = true;
        }
    }

    protected abstract void cancelJobInternal();

    protected String getJobName(String statement) {
        Optional<String> sessionName = context.getSessionName();
        if (sessionName.isPresent()) {
            return String.format("%s:%s:%s", sessionName.get(), sessionId, statement);
        } else {
            return String.format("%s:%s", sessionId, statement);
        }
    }

    @Override
    public JobID getJobId() {
        if (jobId == null) {
            throw new IllegalStateException("No job has been submitted. This is a bug.");
        }
        return jobId;
    }

    @Override
    public synchronized Optional<ResultSet> getJobResult(long token, int maxFetchSize) {
        if (token == currentToken) {
            if (noMoreResults) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Session: {}. There is no more result for job: {}", sessionId, jobId);
                }
                return Optional.empty();
            }

            // a new token arrives, remove used results
            for (int i = 0; i < previousResultSetSize; i++) {
                bufferedResults.removeFirst();
                if (bufferedChangeFlags != null) {
                    bufferedChangeFlags.removeFirst();
                }
            }

            if (bufferedResults.isEmpty()) {
                // buffered results have been totally consumed,
                // so try to fetch new results
                Optional<Tuple2<List<Row>, List<Boolean>>> newResults = fetchNewJobResults();
                if (newResults.isPresent()) {
                    bufferedResults.addAll(newResults.get().f0);
                    if (newResults.get().f1 != null) {
                        if (bufferedChangeFlags == null) {
                            bufferedChangeFlags = new LinkedList<>();
                        }
                        bufferedChangeFlags.addAll(newResults.get().f1);
                    }
                    currentToken++;
                } else {
                    noMoreResults = true;
                    return Optional.empty();
                }
            } else {
                // buffered results haven't been totally consumed
                currentToken++;
            }

            previousMaxFetchSize = maxFetchSize;
            if (maxFetchSize > 0) {
                previousResultSetSize = Math.min(bufferedResults.size(), maxFetchSize);
            } else {
                previousResultSetSize = bufferedResults.size();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Session: {}. Fetching current result for job: {}, token: {}, maxFetchSize: {}, realReturnSize: {}.",
                        sessionId, jobId, token, maxFetchSize, previousResultSetSize);
            }
        } else if (token == currentToken - 1 && token >= 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Session: {}. Fetching previous result for job: {}, token: {}, maxFetchSize: ",
                        sessionId, jobId, token, maxFetchSize);
            }
            if (previousMaxFetchSize != maxFetchSize) {
                String msg = String.format(
                        "As the same token is provided, fetch size must be the same. Expecting max_fetch_size to be %s.",
                        previousMaxFetchSize);
                if (LOG.isDebugEnabled()) {
                    LOG.error(String.format("Session: %s. %s", sessionId, msg));
                }
                throw new SqlPlatformException(msg);
            }
        } else {
            String msg;
            if (currentToken == 0) {
                msg = "Expecting token to be 0, but found " + token + ".";
            } else {
                msg = "Expecting token to be " + currentToken + " or " + (currentToken - 1) + ", but found " + token + ".";
            }
            if (LOG.isDebugEnabled()) {
                LOG.error(String.format("Session: %s. %s", sessionId, msg));
            }
            throw new SqlPlatformException(msg);
        }

        return Optional.of(
                ResultSet.builder()
                        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                        .columns(getColumnInfos())
                        .data(getLinkedListElementsFromBegin(bufferedResults, previousResultSetSize))
                        .changeFlags(getLinkedListElementsFromBegin(bufferedChangeFlags, previousResultSetSize))
                        .build()
        );
    }

    protected abstract Optional<Tuple2<List<Row>, List<Boolean>>> fetchNewJobResults();

    protected abstract List<ColumnInfo> getColumnInfos();

    private <T> List<T> getLinkedListElementsFromBegin(LinkedList<T> linkedList, int size) {
        if (linkedList == null) {
            return null;
        }
        List<T> ret = new ArrayList<>();
        Iterator<T> iter = linkedList.iterator();
        for (int i = 0; i < size; i++) {
            ret.add(iter.next());
        }
        return ret;
    }




}
