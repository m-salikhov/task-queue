import Executor, { IExecutor } from './Executor';
import ITask from './Task';
import { getQueue } from '../test/data';
import queueExt from './extQueue';
import infiniteQueue from './infinityQueue';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
  maxThreads = Math.max(0, maxThreads);

  // Отдельные очереди по targetId. Для контроля очередности исполнения по targetId
  // Каждая очередь обрабатывается в отдельном потоке. hasPendingTask - есть ли исполняющаяся задача в очереди
  const taskQueues: Map<number, { hasPendingTask: boolean; queueTasks: ITask[] }> = new Map();

  // Таски исполняющиеся в данный момент
  const pendingTasks: Map<number, Promise<void>> = new Map();

  // Флаг, что идёт исполнение очереди
  let isConsuming = false;

  //собираем задачи в очереди по targetId и запускаем каждую
  async function consumeQueue(q: AsyncIterable<ITask>) {
    if (isConsuming) {
      // Уже происходит потребление очереди — не запускаемся повторно
      return;
    }

    // отмечаем, что потребление очереди началось
    isConsuming = true;

    try {
      for await (const task of q) {
        // Если достигли лимита maxThreads — ждём завершения хотя бы одной задачи
        if (maxThreads > 0 && pendingTasks.size >= maxThreads) {
          await Promise.race(pendingTasks.values());
        }

        const { targetId } = task;
        const taskQueueByTargetId = taskQueues.get(targetId)?.queueTasks;
        if (!taskQueueByTargetId) {
          // Создаём новую очередь, если ещё нет
          taskQueues.set(targetId, { hasPendingTask: false, queueTasks: [task] });
          processTaskQueue(targetId);
        } else {
          taskQueueByTargetId.push(task);
        }
      }
    } finally {
      // отмечаем, что потребление очереди завершено
      isConsuming = false;
    }
  }

  //обработка отдельной очереди по targetId
  async function processTaskQueue(targetId: number) {
    // получаем очередь по targetId
    const taskQueueData = taskQueues.get(targetId);

    //если очередь не существует или есть активная задача, то ничего не делаем
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
      const task = taskQueueData.queueTasks.shift();

      if (!task) return;

      // запускаем задачу
      const promise = executor.executeTask(task);

      // запоминаем исполняющуюся задачу
      pendingTasks.set(task.targetId, promise);

      // переводим очередь в состояние "есть активная задача"
      taskQueueData.hasPendingTask = true;

      promise.finally(() => {
        // по окончанию задачи:
        // переводим очередь в состояние "нет активных задач",
        // удаляем задачу из исполняющихся,
        // пробуем запустить следующую задачу из той же очереди
        taskQueueData.hasPendingTask = false;
        pendingTasks.delete(targetId);
        processTaskQueue(targetId);

        // пробуем запустить очередь заново - проверка на добавленные задачи
        consumeQueue(queue);
      });
    }
  }

  await consumeQueue(queue);

  // ждём завершения всех оставшихся активных задач
  while (pendingTasks.size > 0) {
    await Promise.all([...pendingTasks.values()]);
  }
}

// run(new Executor(), getQueue());
// run(new Executor(), getQueue(), 2);
// run(new Executor(), getQueue(), 3);
// run(new Executor(), getQueue(), 5);
// run(new Executor(), queueExt, 3);
// run(new Executor(), infiniteQueue, 3);
