import ITaskExt from '../test/ITaskExt';
import ITask from './Task';

const infiniteQueue = {
  [Symbol.asyncIterator]() {
    return {
      async next() {
        const completedCount = infiniteQueue.q.reduce((count, t) => {
          return count + (t.completed ? 1 : 0);
        }, 0);

        const infiniteQueuedCount = infiniteQueue.q.length - completedCount;

        if (completedCount < 20 || infiniteQueuedCount >= 5) {
          const task: ITaskExt = {
            targetId: infiniteQueue.q.length,
            action: 'init',
            acquired: true,
            _onExecute() {
              task.running = true;
            },
            _onComplete() {
              delete task.running;
              task.completed = true;
            },
          };
          task.acquired = true;
          infiniteQueue.q.push(task);
          return {
            done: false,
            value: task,
          };
        } else {
          return {
            done: true,
            value: undefined as unknown as ITask,
          };
        }
      },
    };
  },
  q: [] as ITaskExt[],
};

export default infiniteQueue;
