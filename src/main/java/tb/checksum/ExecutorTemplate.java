package tb.checksum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

public class ExecutorTemplate {

    private volatile ExecutorCompletionService completionService = null;
    private volatile List<Future> futures = null;

    public ExecutorTemplate(Executor executor){
        completionService = new ExecutorCompletionService(executor);
        futures = Collections.synchronizedList(new ArrayList<Future>());
    }

    public void submit(Callable task) {
        Future future = completionService.submit(task);
        futures.add(future);
        check(future);// 立即check下，fast-fail
    }

    public void submit(Runnable task) {
        Future future = completionService.submit(task, null);
        futures.add(future);
        check(future);// 立即check下，fast-fail
    }

    private void check(Future future) {
        if (future.isDone()) {
            // 立即判断一次，因为使用了CallerRun可能当场跑出结果，针对有异常时快速响应，而不是等跑完所有的才抛异常
            try {
                future.get();
            } catch (InterruptedException e) {
                cancel();// 取消完之后立马退出
                throw new RuntimeException(e);
            } catch (Throwable e) {
                cancel(); // 取消完之后立马退出
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized List<?> waitForResult() {
        List result = new ArrayList();
        RuntimeException exception = null;
        // 开始处理结果
        int index = 0;
        while (index < futures.size()) { // 循环处理发出去的所有任务
            try {
                Future future = completionService.take();// 它也可能被打断
                result.add(future.get());
            } catch (InterruptedException e) {
                exception = new RuntimeException(e);
                break;// 如何一个future出现了异常，就退出
            } catch (Throwable e) {
                exception = new RuntimeException(e);
                break;// 如何一个future出现了异常，就退出
            }

            index++;
        }

        if (exception != null) {
            // 小于代表有错误，需要对未完成的记录进行cancel操作，对已完成的结果进行收集，做重复录入过滤记录
            cancel();
            throw exception;
        } else {
            return result;
        }
    }

    public void cancel() {
        for (int i = 0; i < futures.size(); i++) {
            Future future = futures.get(i);
            if (!future.isDone() && !future.isCancelled()) {
                future.cancel(true);
            }
        }
    }

    public void clear() {
        futures.clear();
    }
}
