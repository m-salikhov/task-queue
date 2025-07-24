import Executor, { IExecutor } from './Executor';
import ITask from './Task';
import { getQueue } from '../test/data';
import queueExt from './extQueue';
import infiniteQueue from './infinityQueue';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
  maxThreads = Math.max(0, maxThreads);

  // Разбивает очередь задач на отдельные очереди по targetId. Для контроля очередности исполнения по targetId
  // Каждая очередь обрабатывается в отдельном потоке. hasPendingTask - есть ли исполняющаяся задача в очереди
  const taskQueues: Map<number, { hasPendingTask: boolean; queueTasks: ITask[] }> = new Map();

  // Таски исполняющиеся в данный момент
  const pendingTasks: Map<number, Promise<void>> = new Map();
  // const completedTasks = new Set<string>();

  //собираем задачи в очереди по targetId и запускаем каждую
  async function consumeQueue(q: AsyncIterable<ITask>) {
    for await (const task of q) {
      // если максимальное количество активных задач достигнуто, ждем завершения одной
      if (maxThreads > 0 && pendingTasks.size >= maxThreads) {
        await Promise.race(pendingTasks.values());
      }

      // console.log('TASK IN CYCLE ' + task.targetId + task.action);

      const { targetId } = task;

      if (!taskQueues.has(targetId)) {
        //создаем очередь, если ещё нет. И запускаем эту очередь
        taskQueues.set(targetId, { hasPendingTask: false, queueTasks: [task] });
        processTaskQueue(targetId);
      } else {
        //добавляем задачу в очередь, если есть очередь
        taskQueues.get(targetId)!.queueTasks.push(task);
      }
    }

    // console.log('cycle END');
  }

  //обработка отдельной очереди по targetId
  async function processTaskQueue(targetId: number) {
    // console.log(' processTaskQueue ~ targetId:', targetId);
    // получаем очередь по targetId
    const taskQueueData = taskQueues.get(targetId);

    //если есть активная задача или очередь не существует, то ничего не делаем
    if (!taskQueueData || taskQueueData.hasPendingTask) {
      return;
    }

    // Если очередь пуста, удаляем ее из taskQueues
    if (taskQueueData.queueTasks.length === 0) {
      taskQueues.delete(targetId);
      return;
    }

    if (maxThreads > 0 && pendingTasks.size >= maxThreads) {
      // если максимальное количество активных задач достигнуто, ждем завершения одной
      await Promise.race(pendingTasks.values());
      // снова запускаем обработку очереди по данному targetId
      processTaskQueue(targetId);
    } else {
      const task = taskQueueData.queueTasks.shift()!;
      // completedTasks.add(task.targetId + task.action);

      let promise = executor.executeTask(task).finally(() => {
        // по окончанию задачи:
        // переводим очередь в состояние "нет активных задач",
        // удаляем задачу из исполняющихся,
        // пробуем запустить следующую задачу из той же очереди
        taskQueueData.hasPendingTask = false;
        pendingTasks.delete(targetId);
        processTaskQueue(targetId);
        if (pendingTasks.size === 0) {
          consumeQueue(queue);
        }
      });

      // запоминаем исполняющуюся задачу
      pendingTasks.set(task.targetId, promise);

      // переводим очередь в состояние "есть активная задача"
      taskQueueData.hasPendingTask = true;
    }
  }

  await consumeQueue(queue);

  // ждём завершения всех активных задач
  while (pendingTasks.size > 0) {
    // console.log('waiting for pending tasks');
    await Promise.all([...pendingTasks.values()]);
  }
}

run(new Executor(), getQueue());
// run(new Executor(), getQueue(), 2);
// run(new Executor(), getQueue(), 3);
// run(new Executor(), getQueue(), 5);
// run(new Executor(), queueExt, 3);
// run(new Executor(), infiniteQueue, 3);
