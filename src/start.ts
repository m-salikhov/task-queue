import Executor, { IExecutor } from './Executor';
import ITask from './Task';
import { getQueue } from '../test/data';
import queueExt from './extQueue';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>) {
  // maxThreads = Math.max(0, maxThreads);

  const taskQueues: Map<number, ITask[]> = new Map();
  const activeTask: Set<number> = new Set();
  const pendingPromises = new Set<Promise<void>>();

  async function processTask(task: ITask) {
    activeTask.add(task.targetId);

    // console.log(taskQueues.keys());
    const taskPromise = executor.executeTask(task).finally(() => {
      activeTask.delete(task.targetId);

      if (taskQueues.has(task.targetId) && taskQueues.get(task.targetId)!.length > 0) {
        const nextPromise = processTask(taskQueues.get(task.targetId)!.shift()!);
        pendingPromises.add(nextPromise);
        nextPromise.finally(() => pendingPromises.delete(nextPromise));
      } else {
        taskQueues.delete(task.targetId);
      }
    });

    pendingPromises.add(taskPromise);
    await taskPromise.finally(() => pendingPromises.delete(taskPromise));
  }

  async function consumeQueue(q: AsyncIterable<ITask>) {
    for await (const task of q) {
      if (activeTask.has(task.targetId)) {
        if (taskQueues.has(task.targetId)) {
          taskQueues.get(task.targetId)!.push(task);
        } else {
          taskQueues.set(task.targetId, [task]);
        }
      } else {
        activeTask.add(task.targetId);
        processTask(task);
      }
    }

    while (pendingPromises.size > 0) {
      console.log('pendingPromises', pendingPromises);
      await Promise.all([...pendingPromises]);
    }
  }

  await consumeQueue(queue);
}

run(new Executor(), getQueue());
// run(new Executor(), queueExt, 3);
// console.log(getQueue());
