gm_utils = {};

(function async() {
    async function sleep(ms) {
        return new Promise(res => setTimeout(ms, res));
    }
    function to_async(f) {
        return async (...args) => {
            await sleep(0);
            const r = f.apply(args);
            await sleep(0);
            return r;
        }
    }
    function queue_async(f) {
        const fa = to_async(f);
        fa();
    }
    
    function fthread(f0, ...fs) {
        return (...args) => {
            let v = f0.apply(args);
            for (let i = 0; i < fs.length; ++i) {
                v = fs[i](v);
            }
            return v;
        }
    }
    function fthread_async(f0, ...fs) {
        return async (...args) => {
            let v = await to_async(f0).apply(args);
            for (let i = 0; i < fs.length; ++i) {
                v = await to_async(fs[i])(v);
            }
            return v;
        }
    }

    gm_utils.gen = {
        sleep,
        to_async, queue_async,
        fthread, fthread_async,
    }; 
})()

(function async_sync() {
    const locks = { next: 0 };
    function make_lock_name(name_opt) {
        if (name_opt === undefined) name_opt = "";
        else name_opt = "_" + name_opt;
        
        const name = `lock_${locks.next++}${name_opt}`;
        locks[name] = 1;
        return name;
    }
    function lock_try_acquire(lknm) {
        if (locks[lknm]) {
            --locks[lknm];
            return true;
        } else {
            return false;
        }
    }
    async function lock_acquire(lknm) {
        if (locks[lknm] === 0) {
            await resqueue_push(`lock_${lknm}`);
        }
        --locks[lknm];
    }
    function lock_release(lknm) {
        ++locks[lknm];
        resqueue_pop(`lock_${lknm}`);
    }

    function make_lock(name_opt) {
        const k = make_lock_name(name_opt);
        const ak = async () => await lock_acquire(k);
        const tak = () => lock_try_release(k);
        const rel = () => lock_release(k);
        const bound = async (f) => {
            await ak();
            const r = await f();
            rel();
            return r;
        };
        return {
            _desc: `mutex -- semaphore-based critical-section synchronization.
                    Starts unlocked (semaphore 1). Contention resolved round-robin.
                    - async .acquire(), async .mx_acquire()
                    - .try_acquire(), .mx_try_acquire()
                    - .release(), .mx_try_release()
                    - .peek() => int
                    `,
            key: k,
            acquire: ak,        mx_acquire: ak,
            try_acquire: tak,   mx_try_acquire: tak,
            release: rel,       mx_release: rel,
            with_lock: bound,   mx_with_lock: bound,
            peek: () => locks[k],
        };
    }


    const resqueues = { next: 0 };
    function make_resqueue_name(name) {
        if (name_opt === undefined) name_opt = "";
        else name_opt = "_" + name_opt;
        
        const name = `queue_${locks.next++}${name_opt}`;
        return name;
    }
    async function resqueue_push(key) {
        let res;
        const p = new Promise(r => { res = r; });
        
        resqueues[key] = resqueues[key] ?? [];
        resqueues[key].push(res);
        
        if (resqueues[key].length === 1) res();

        return p;
    }
    function resqueue_pop(key) {
        resqueues[key] = resqueues[key] ?? [];
        resqueues[key].shift();
        if (resqueues[key].length > 0)
            resqueues[key][0]();
    }

    function make_resqueue(name_opt) {
        const k = make_resqueue_name(name_opt);
        const wait = async () => await resqueue_push(k);
        const signal = () => resqueue_pop(k);
        return {
            _desc: `Condition variable -- managed queue-based synchronization.
                    - async .wait(), async .cv_wait()
                        Put current thread of execution onto the cv's queue,
                        blocking until signalled
                    - .signal(), .cv_signal()
                        Signal this cv, unblocking its first waiter`,
            key: k,
            wait: wait,     cv_wait: wait,
            signal: signal, cv_signal: signal,
        };
    }
    
    gm_utils.sync = {
        mutex: make_lock,
        condvar: make_resqueue,
    };
})();

/*
(function simple_storage_old() {
    const key_main      = 'STR_MAIN';
    const key_IR        = 'STR_IR';
    const key_writelog  = 'STR_WRITELOG';
    const _CPEO = LZString.compressToUTF16("{}"); 
    const defser = { [key_main]: _CPEO, [key_IR]: _CPEO, [key_writelog]: "[]" };

    const to_async = gm_utils.gen.to_async;

    const serialize   = to_async(JSON.stringify);
    const deserialize = to_async(JSON.parse);
    const compress    = to_async(LZString.compressToUTF16);
    const decompress  = to_async(LZString.decompressFromUTF16);
    const serialize_compress   = fthread_async(serialize, compress);
    const deserialize_compress = fthread_async(decompress, deserialize);

    async function kv_read_raw(...keys) {
        const kvs = keys.map(k => [k, defser[k] ?? "null" ]);
        const spec = await Object.fromEntries(kvs);
        return await GM.getValues(spec);
    }
    async function kv_read(...keys) {
        const u = kv_read_raw.apply(null, keys);
        const r = {};
        if (u[key_main] !== undefined) r.main = await deserialize_compress(u[key_main]);
        if (u[key_IR]   !== undefined) r.IR   = await deserialize_compress(u[key_IR]);
        if (u[key_writelog] !== undefined) r.writelog = await deserialize(u[key_writelog]);
        return r;
    }
    async function kv_write(u) {
        const r = {};
        if (u.main !== undefined)    r[key_main] = await serialize_compress(u.main);
        if (u.IR   !== undefined)    r[key_IR]   = await serialize_compress(u.IR);
        if (u.writelog !== undefined) r[key_writelog] = await serialize(u.writelog);
        await GM.setValues(r);
    }

    async function do_write(u) {
        const r = await kv_read(key_writelog);

        r.writelog.push(u);

        await kv_write(r);
    }
    async function do_read() {
        const r = await kv_read(key_main, key_IR, key_writelog);

        let u = r.main;
        u = await patch(u, r.IR);
        for (let i = 0; i < r.writelog.length; ++i) {
            u = await patch(u, r.writelog[i]);
        }

        return u;
    }
    async function do_update() {
        const r = await kv_read(key_main, key_IR, key_writelog);
        
        for (let i = 0; i < r.writelog.length; ++i) {
            r.IR = await patch(r.IR, r.writelog[i]);
        }
        r.writelog = [];

        await kv_write(r);

        r.main = await patch(r.main, r.IR);
        r.IR = {};

        await kv_write(r);
    }

    // levels: r[collection][key] = value
    function patch(a, b) {
        function patch_on_collection(a, b) {
            const ret = Object.assign({}, a, b);
            Object.keys(b).forEach(k => {
                if (b[k] === undefined) delete ret[k];
            });
            return ret;
        }

        const ret = {};
        Object.keys(a).forEach(k => {
            if (b[k] === undefined) ret[k] = a[k];
        });
        Object.keys(b).forEach(k => {
            if (b[k] === undefined) delete ret[k];
            else ret[k] = patch_on_collection(a[k], b[k]);
        });
        return ret;
    }
    function diff(a, b) {
        function diff_on_collection(a, b) {
            const ret = {};
            Object.keys(a).forEach(k => {
                if (b[k] === undefined) ret[k] = undefined;
            });
            Object.keys(b).forEach(k => {
                if (b[k] !== a[k]) ret[k] = b[k];
            });
            return ret;
        }

        const ret = {};
        Object.keys(a).forEach(k => {
            if (b[k] === undefined) ret[k] = undefined;
        });
        Object.keys(b).forEach(k => {
            ret[k] = diff_on_collection(a[k], b[k]);
        });
        return ret;
    }

    let cache = null;
    const mx_cache = gm_utils.sync.mutex("gm_utils.storage.cache");
    async function load() {
        cache = await mx_cache.with_lock(do_read);
        return structuredClone(cache);
    }
    async function commit(db) {
        if (cache === null) return;

        await mx_cache.with_lock(async () => {
            const db_diff = diff(cache, db);
            await do_write(db_diff);
            cache = structuredClone(db);
        });

        update();
    }

    async function update() {
        
    }

    gm_utils.storage = {
        load: 0,
        save: 0,
    };
})()
*/

(function simple_storage() {
    const key_main      = 'STR_MAIN';
    const key_IR        = 'STR_IR';
    const key_writelog  = 'STR_WRITELOG';
    const _CPEO = LZString.compressToUTF16("{}"); 
    const defser = { [key_main]: _CPEO, [key_IR]: _CPEO, [key_writelog]: "[]" };

    const to_async = gm_utils.gen.to_async;

    const serialize   = to_async(JSON.stringify);
    const deserialize = to_async(JSON.parse);
    const compress    = to_async(LZString.compressToUTF16);
    const decompress  = to_async(LZString.decompressFromUTF16);
    const serialize_compress   = fthread_async(serialize, compress);
    const deserialize_compress = fthread_async(decompress, deserialize);

    async function kv_read_raw(...keys) {
        const kvs = keys.map(k => [k, defser[k] ?? "null" ]);
        const spec = await Object.fromEntries(kvs);
        return await GM.getValues(spec);
    }
    async function kv_read(...keys) {
        const u = kv_read_raw.apply(null, keys);
        const r = {};
        if (u[key_main] !== undefined) r.main = await deserialize_compress(u[key_main]);
        if (u[key_IR]   !== undefined) r.IR   = await deserialize_compress(u[key_IR]);
        if (u[key_writelog] !== undefined) r.writelog = await deserialize(u[key_writelog]);
        return r;
    }
    async function kv_write(u) {
        const r = {};
        if (u.main !== undefined)    r[key_main] = await serialize_compress(u.main);
        if (u.IR   !== undefined)    r[key_IR]   = await serialize_compress(u.IR);
        if (u.writelog !== undefined) r[key_writelog] = await serialize(u.writelog);
        await GM.setValues(r);
    }

    async function do_write(u) {
        const r = await kv_read(key_writelog);

        r.writelog.push(u);

        await kv_write(r);
    }
    async function do_read() {
        const r = await kv_read(key_main, key_IR, key_writelog);

        let u = r.main;
        u = await patch(u, r.IR);
        for (let i = 0; i < r.writelog.length; ++i) {
            u = await patch(u, r.writelog[i]);
        }

        return u;
    }
    async function* do_update() {
        yield;
        const r = await kv_read(key_main, key_IR, key_writelog);
        
        for (let i = 0; i < r.writelog.length; ++i) {
            r.IR = await patch(r.IR, r.writelog[i]);
        }
        r.writelog = [];

        yield;
        await kv_write(r);
        yield;

        r.main = await patch(r.main, r.IR);
        r.IR = {};

        yield;
        await kv_write(r);
    }

    // levels: r[collection][key] = value
    function patch(a, b) {
        function patch_on_collection(a, b) {
            const ret = Object.assign({}, a, b);
            Object.keys(b).forEach(k => {
                if (b[k] === undefined) delete ret[k];
            });
            return ret;
        }

        const ret = {};
        Object.keys(a).forEach(k => {
            if (b[k] === undefined) ret[k] = a[k];
        });
        Object.keys(b).forEach(k => {
            if (b[k] === undefined) delete ret[k];
            else ret[k] = patch_on_collection(a[k], b[k]);
        });
        return ret;
    }
    function diff(a, b) {
        function diff_on_collection(a, b) {
            const ret = {};
            Object.keys(a).forEach(k => {
                if (b[k] === undefined) ret[k] = undefined;
            });
            Object.keys(b).forEach(k => {
                if (b[k] !== a[k]) ret[k] = b[k];
            });
            return ret;
        }

        const ret = {};
        Object.keys(a).forEach(k => {
            if (b[k] === undefined) ret[k] = undefined;
        });
        Object.keys(b).forEach(k => {
            ret[k] = diff_on_collection(a[k], b[k]);
        });
        return ret;
    }

    let cache = null;
    let commit_clear = true; // has successfully committed
    const mx_cache = gm_utils.sync.mutex("gm_utils.storage.cache");
    const mx_write = gm_utils.sync.mutex("gm_utils.storage.write");
    async function load() {
        cache = await mx_cache.with_lock(do_read);
        return structuredClone(cache);
    }
    async function commit(db) {
        if (cache === null) return;
        if (!commit_clear) return;

        commit_clear = false;
        await mx_write.with_lock(async() => {
            await mx_cache.with_lock(async () => {
                const db_diff = diff(cache, db);
                await do_write(db_diff);
                cache = structuredClone(db);
            });
        });

        gm_utils.gen.queue_async(update);
        commit_clear = true;
    }

    async function update() {
        await mx_write.with_lock(async() => {
            const updater = do_update();
            do {
                const { done } = await mx_cache.with_lock(updater.step);
            } while (!done && commit_clear);
        });
    }

    gm_utils.storage = {
        load, commit,
    };
})()
