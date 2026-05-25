"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Mutex = void 0;
class Mutex {
    constructor() {
        this.locked = false;
        this.queue = [];
    }
    async acquire() {
        return new Promise((resolve) => {
            const tryAcquire = () => {
                if (!this.locked) {
                    this.locked = true;
                    resolve(() => this.release());
                }
                else {
                    this.queue.push(tryAcquire);
                }
            };
            tryAcquire();
        });
    }
    release() {
        this.locked = false;
        const next = this.queue.shift();
        if (next)
            next();
    }
}
exports.Mutex = Mutex;
//# sourceMappingURL=mutex.js.map