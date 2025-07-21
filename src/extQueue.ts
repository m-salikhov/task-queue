import ITaskExt from '../test/ITaskExt';
import { ActionType } from './Task';

const tasks: ITaskExt[][] = [];
for (const i of [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]) {
  tasks[i] = [];
  for (const action of ['init', 'prepare', 'work', 'finalize', 'cleanup']) {
    tasks[i].push({
      targetId: i,
      action: action as ActionType,
      _onExecute() {
        this.running = true;
      },
      _onComplete() {
        delete this.running;
        this.completed = true;
      },
    });
  }
}

const q = [...tasks[0]];
tasks[0][4]._onComplete = () => {
  q.push(...tasks[1], ...tasks[2], ...tasks[3]);

  delete tasks[0][4].running;

  tasks[0][4].completed = true;
};
tasks[1][1]._onComplete = () => {
  q.push(...tasks[4]);
  delete tasks[1][1].running;
  tasks[1][1].completed = true;
};
tasks[2][2]._onComplete = () => {
  q.push(...tasks[5]);
  delete tasks[2][2].running;
  tasks[2][2].completed = true;
};
tasks[3][3]._onComplete = () => {
  q.push(...tasks[6]);
  delete tasks[3][3].running;
  tasks[3][3].completed = true;
};
tasks[4][4]._onComplete = () => {
  q.push(...tasks[7]);
  delete tasks[4][4].running;
  tasks[4][4].completed = true;
};
tasks[5][4]._onComplete = () => {
  q.push(...tasks[8]);
  delete tasks[5][4].running;
  tasks[5][4].completed = true;
};
tasks[8][4]._onComplete = () => {
  q.push(...tasks[9], ...tasks[10], ...tasks[11]);
  delete tasks[8][4].running;
  tasks[8][4].completed = true;
};

const queueExt = {
  [Symbol.asyncIterator]() {
    let i = 0;
    return {
      async next() {
        while (q[i] && (q[i].completed || q[i].acquired)) {
          i++;
        }
        if (i < q.length) {
          const value = q[i++];
          if (value) {
            value.acquired = true;
          }
          return {
            done: false,
            value,
          };
        } else {
          return {
            done: true,
            value: undefined as unknown as ITaskExt,
          };
        }
      },
    };
  },
  q,
};

export default queueExt;
