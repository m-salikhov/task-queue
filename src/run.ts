import Executor, { IExecutor } from './Executor';
import ITask from './Task';
import { getQueue } from '../test/data';
import queueExt from './extQueue';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
  maxThreads = Math.max(0, maxThreads);

  const taskQueues: Map<number, { hasActiveTask: boolean; queueTasks: ITask[] }> = new Map();
  const activeTask: Map<number, Promise<void>> = new Map();
  // const completedTasks = new Set<string>();

  //собираем задачи в очереди по targetId и запускаем каждую
  async function consumeQueue(q: AsyncIterable<ITask>) {
    for await (const task of q) {
      // if (completedTasks.has(task.targetId + task.action)) {
      //   console.log(task.targetId + task.action);
      //   continue;
      // }

      const { targetId } = task;

      if (!taskQueues.has(targetId)) {
        taskQueues.set(targetId, { hasActiveTask: false, queueTasks: [task] });
        processTaskQueue(targetId);
      } else {
        taskQueues.get(targetId)!.queueTasks.push(task);
      }
    }

    while (taskQueues.size > 0) {
      await new Promise((resolve) => setTimeout(resolve));
    }

    for await (const task of q) {
      await executor.executeTask(task);
      await consumeQueue(q);
    }
  }

  //обработка отдельной очереди по targetId
  async function processTaskQueue(targetId: number) {
    // console.log(' processTaskQueue ~ targetId:', targetId);

    const taskQueueData = taskQueues.get(targetId);
    //если есть активная задача, то ничего не делаем
    if (!taskQueueData || taskQueueData.hasActiveTask) {
      return;
    }

    // Если очередь пуста, удаляем ее из taskQueues
    if (taskQueueData.queueTasks.length === 0) {
      taskQueues.delete(targetId);
      return;
    }

    if (maxThreads > 0 && activeTask.size >= maxThreads) {
      await Promise.race(activeTask.values());
      processTaskQueue(targetId);
    } else {
      const task = taskQueueData.queueTasks.shift()!;
      // completedTasks.add(task.targetId + task.action);

      let promise = executor.executeTask(task).finally(() => {
        taskQueueData.hasActiveTask = false;

        activeTask.delete(targetId);

        processTaskQueue(targetId);
      });

      activeTask.set(task.targetId, promise);

      taskQueueData.hasActiveTask = true;
    }
  }

  await consumeQueue(queue);
}

// run(new Executor(), getQueue(), 3);
// run(new Executor(), queueExt, 3);
