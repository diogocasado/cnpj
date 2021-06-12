#!/usr/bin / env node

const Fs = require('fs').promises;

const Options = {
    v: { value: false, type: 'flag', description: 'Verbose' },
    in: { value: null, type: 'string', description: 'Input file' },
    m: { value: null, type: 'string', description: 'Match records: `field:startsWith, field:start-end`' },
    or: { value: false, type: 'flag', description: 'Match rules using OR' },
    t: { value: 'csv', type: 'string', description: 'Output type: csv (default), json' },
    sep: { value: ',', type: 'string', description: 'CSV field separator'},
    del: { value: '"', type: 'string', description: 'CSV field delimiter' },
    nil: { value: '', type: 'string', description: 'CSV nil field value'},
    csv: { value: 'razaoSocial,tipoEndereco,endereco,number,complemento,bairro,uf,cep,municipio,telefone1,telefone2,email,socios[].tipo,socios[].codPais,socios[].nome', type: 'string', description: 'CSV field list: fields, field' },
    ml: { value: false, type: 'flag', description: 'Output matching lines only'},
    out: { value: null, type: 'string', description: 'Output file' }
}

var cmd = [];

function parseCmdline () {
    var args = process.argv.slice(2);

    for (var idx = 0; idx < args.length; idx++) {
        let arg = args[idx];

        if (arg.startsWith('-')) {
            let option = arg.slice(1);

            if (typeof Options[option] !== 'undefined') {
                let type = Options[option].type;
                var value = null;

                if (type == 'int') {
                    value = parseInt(args[++idx]);
                } else if (type == 'flag') {
                    value = true;
                } else if (type == 'string') {
                    value = args[++idx];
                } else {
                    throw 'Invalid argument type: ' + type;
                }

                //console.log(_.join(('set', option, type, value), ' '));

                if (value == NaN)
                    throw 'Option ' + option + ': Invalid value';

                Options[option].value = value;

            } else {
                throw 'Unkown option: ' + option;
            }
        } else {
            cmd.push(arg);
        }
    }

    return Promise.resolve(Options);
}

function exec () {

    if (cmd.length > 0) {
        if (cmd[0] == 'help') return help();
        if (cmd[0] == 'parse') return parse();
        throw 'Unkown commmand: ' + cmd;
    } else {
        return help();
    }    
}

const isVerbose = () => !!Options.v.value;

function log () {
    if (Options.v.value)
        console.log(...arguments);
}

function parse () {
    return Promise.resolve({})
        .then(openInFile)
        .then(openOutFile)
        .then(setupRules)
        .then(nextRecord)
        .then(finish);
}

const RESOLVE_PROPERTY_EXPR = /^(\w+?)(\[(\~|\d+)\])?$/g;
function parsePathNode (node) {
    RESOLVE_PROPERTY_EXPR.lastIndex = 0;
    let match = RESOLVE_PROPERTY_EXPR.exec(node);
    if (match) return {
        property: match[1],
        selector: match[3] || match[2]
    };
}

function resolveObj (root, path = '') {
    const nodes = Array.isArray(path) ? path : path.trim().split('.');
    let obj = root;

    for (let node of nodes) {
        if (node.length == 0)
            break;
        node = parsePathNode(node);
        obj = obj[node.property];
        if (typeof obj === 'undefined' || obj === null)
            return null;
        if (node.selector === '~')
            obj = obj[obj.length - 1];
        else if (node.selector)
            obj = obj[node.selector];
    }

    return obj;
}

function attributeObj (root, path = '', value) {
    const nodes = Array.isArray(path) ? path : path.trim().split('.');
    const node = parsePathNode(nodes.pop());

    let obj = resolveObj(root, nodes);
    if (obj) {
        let property = node.property;
        if (node.selector) {
            obj = obj[node.property];
            if (node.selector === '[~]')
                property = obj.length - 1;
            else
                property = node.selector;
        }

        obj[property] = value;
    } else {
        console.log('attributeObj: Cannot set "' + path + '"');
    }
}

function* pathIterator (root, path) {
    const nesting = path.split('.');
    const it = [];

    let depth = 0;
    let obj = root;
    while (depth >= 0) {
        if (it[depth]) {
            let next = it[depth].next();
            it[depth].index++;
            if (next.done) {
                it[depth] = null;
                while (depth >= 0 && !it[depth])
                    depth--;
            } else {
                obj = next.value;
                depth++;
            }
        } else if (depth < nesting.length) {
            if (nesting[depth].endsWith('[]')) {
                const itObj = resolveObj(obj, nesting[depth].slice(0, -2));
                it[depth] = itObj[Symbol.iterator]();
                it[depth].index = -1;
            } else {
                obj = resolveObj(obj, nesting[depth]);
                depth++;
            }
        } else {
            let abspath = [];
            for (let absindex = 0; absindex < depth; absindex++) {
                const node = nesting[absindex];
                if (node.endsWith('[]')) {
                    let absnode = node.substring(0, node.length - 2);
                    absnode += '[' + it[absindex].index + ']';
                    abspath.push(absnode);
                } else {
                    abspath.push(node);
                }
            }
            abspath = abspath.join('.');
            log("Iterate " + abspath);
            yield {
                path: abspath,
                data: obj
            };
            while (depth >= 0 && !it[depth])
                depth--;
        }
    }
}

const RULES = [
    matchStartsWith,
    matchRange
];

function setupRules (ctx) {
    ctx.rules = [];
    if (Options.m.value) {
        let specs = Options.m.value.split(',');
        for (let spec of specs) {
            let valid = false;
            for (let rule of RULES) {
                let handler = rule(spec);
                if (typeof handler === 'function') {
                    ctx.rules.push(handler);
                    valid = true;
                }
            }
            if (!valid)
                throw 'Unrecognized match spec: ' + spec;
        }
    }
    return ctx;
}

const MATCH_STARTS_WITH_EXPR = /^(.+?)\:(\w+?)$/g;
function matchStartsWith (spec) {
    MATCH_STARTS_WITH_EXPR.lastIndex = 0;
    let match = MATCH_STARTS_WITH_EXPR.exec(spec);
    if (match) {
        const path = match[1];
        const value = match[2];
        log('Rule ' + path + ':' + value);
        return (obj, ctx) => {
            let result = false;
            for (let target of pathIterator(obj, path)) {
                let data = target.data;
                log('matchStartsWith ' + data + ' ? ' + value);
                if (typeof data !== 'undefined' &&
                    data.startsWith(value)) {
                    result = true;
                    ctx.matches.push(target.path);
                }
            }
            return result;
        };
    }
}

const MATCH_RANGE_EXPR = /^(.+?)\:(\d+?)-(\d+?)$/g;
function matchRange (spec) {
    MATCH_RANGE_EXPR.lastIndex = 0;
    let match = MATCH_RANGE_EXPR.exec(spec);
    if (match) {
        const path = match[1];
        const start = +match[2];
        const end = +match[3];
        const len = Math.max(match[2].trim().length, match[3].trim().length);
        log('Rule ' + property + ':' + start + '-' + end + ' (' + len + ')');
        return (obj, ctx) => {
            let result = false;
            for (let target of pathIterator(obj, path)) {
                let data = target.data;
                if (typeof data === 'string')
                    data = data.substring(0, len);
                log('matchRange ' + data + ' ? ' + start + '-' + end);
                if (typeof data !== 'undefined' &&
                    data >= start && data <= end) {
                    result = true;
                    ctx.matches.push(target.path);
                }
            }
            return result;
        };
    }
}

function openInFile (ctx) {
    if (!Options.in.value)
        throw 'Missing -in parameter';
    return Fs.open(Options.in.value, 'r')
      .then((fh) => {
        ctx.inf = fh;
        ctx.inpos = 0;
        ctx.inCnt = 0;
        ctx.outCnt = 0;
        return ctx.inf.stat().then((stats) => {
          ctx.infStats = stats;
          return ctx;
        });
    });
}

function openOutFile (ctx) {
    if (Options.out.value) {
        return Fs.open(Options.out.value, 'wx+')
          .then((fh) => {
            ctx.outf = fh;
            return ctx;
        });
    } else {
        log('Using console output.');
    }
    return ctx;
}

function nextRecord (ctx) {
    if (ctx.finish)
        return Promise.resolve(ctx);
    return Promise.resolve(ctx)
        .then(readRecord)
        .then(parseRecord)
        .then(writeOutObj)
        .then(logProgress)
        .then(nextRecord);
}

function readRecord (ctx) {
    ctx.inb = Buffer.alloc(1201);
    return ctx.inf.read(ctx.inb, 0, ctx.inb.length, ctx.inpos).then((result) => {
        log('Read ' + result.bytesRead);
        ctx.inlen = result.bytesRead;
        ctx.inpos += result.bytesRead;
        if (ctx.inlen == 0)
            ctx.finish = true;
        return ctx;
    });
}

const RECORD_HEADER = '0';
const RECORD_PRINCIPAL = '1';
const RECORD_SOCIOS = '2';
const RECORD_CNAES = '6';
const RECORD_TRAILLER = '9';

// According to: Nov/20 2018 specs
const DEFS = {
    [RECORD_HEADER]: null,
    [RECORD_PRINCIPAL]: {
        _onRecord: [
            matchObj('in', 'out'),
            newObj('in')
        ],
        _path: 'in',
        'cnpj': field(3, 14),
        'razaoSocial': field(19, 150),
        'nomeFantasia': field(169, 55),
        'situacao': field(224, 2),
        'tipoEndereco': field(383, 20),
        'endereco': field(403, 60),
        'number': field(463, 6),
        'complemento': field(469, 156),
        'bairro': field(625, 50),
        'cep': field(675, 8),
        'uf': field(683, 2),
        'municipio': field(689, 50),
        'telefone1': field(739, 12),
        'telefone2': field(751, 12),
        'email': field(775, 115),
        'socios': newArray
    },
    [RECORD_SOCIOS]: {
        _onRecord: [
            newObj('in.socios')
        ],
        _path: 'in.socios[~]',
        'tipo': field(18, 1),
        'nome': field(19, 150),
        'codPais': field(198, 3),
        'nomePais': field(201, 70),
        'nomeRepr': field(282, 60)
    },
    [RECORD_CNAES]: null, // Don't care for now
    [RECORD_TRAILLER]: {
        _onRecord: [
            stop()
        ],
        path: 'trailler',
        'totalt1': field(18, 9),
        'totalt2': field(27, 9),
        'totalt3': field(36, 9),
        'total': field(45, 11)
    }
};

function parseRecord (ctx) {
    if (ctx.inlen == 0)
        return ctx;
    if (ctx.inlen < 1200)
        throw 'Record too small (' + ctx.inlen + ').';
    
    ctx.record = ctx.inb.toString('ascii');
    const type = ctx.record.charAt(0);

    log('Record ' + ctx.record);
    log('Type ' + type);

    const def = DEFS[type];
    if (typeof def !== 'undefined') {
        if (typeof def === 'object' &&
                   def != null) {
            parseStructure(ctx, def);
        } else if (typeof def === 'function') {
            def(ctx);
        }
    } else {
        console.warn('Unknown record ' + type);
    }
    
    ctx.inCnt++;

    return ctx;
}

function parseStructure (ctx, struct) {
    if (typeof struct._onRecord === 'function') {
        struct._onRecord(ctx);
    } else if (Array.isArray(struct._onRecord)) {
        for (let handler of struct._onRecord)
            handler(ctx);
    }
    const obj = resolveObj(ctx, struct._path);
    if (obj) {
        for (let field in struct) {
            if (field.startsWith('_'))
                continue;
            let op = struct[field];
            if (typeof op === 'function') {
                let value = op(ctx);
                if (value !== '')
                    obj[field] = value;
            }
        }
    } else {
        console.log('parseStructure: Cannot get path "' + struct._path + '"');
    }
}

function field (start, len, trim = true) {
    return (ctx) => {
        let value = ctx.record.substring(start - 1, start - 1 + len);
        return trim ? value.trim() : value;
    };
}

function stop () {
    return (ctx) => {
        ctx.finish = true;
        return ctx;
    };
}

function matchObj (inPath, outPath) {
    return (ctx) => {
        ctx.matches = [];
        const obj = resolveObj(ctx, inPath);
        if (typeof obj !== 'undefined' && obj !== null) {
            let result = true;
            for (let rule of ctx.rules) {
                result = rule(obj, ctx);
                if (!result && !Options.or.value)
                    break;
                if (result && Options.or.value)
                    break;
            }
            if (result)
                attributeObj(ctx, outPath, obj);
        }
        return ctx;
    }
}

function setObj (targetPath, sourcePath) {
    const path = targetPath.trim().split('.');
    const relPath = path.slice(0, -1);
    const property = path.slice(-1);
    return (ctx) => {
        const target = resolveObj(ctx, relPath);
        target[property] = resolveObj(ctx, sourcePath);
    }
}

function newObj (path) {
    path = path.trim().split('.');
    const relPath = path.slice(0, -1);
    const property = path.slice(-1);
    return (ctx) => {
        const obj = resolveObj(ctx, relPath);
        if (typeof obj !== 'undefined' && obj !== null) {
            if (Array.isArray(obj[property]))
                obj[property].push({});
            else
                obj[property] = {};
        } else {
            console.log('newObj: Cannot resolve ' + relPath);
        }
    }
}

function newArray () {
    return [];
}

function writeOutObj (ctx) {
    if (ctx.out && ctx.lastWritten !== ctx.out) {
        if (isVerbose())
            console.log(JSON.stringify(ctx.out));
        let ret = OUTPUTS[Options.t.value](ctx);
        ctx.lastWritten = ctx.out;
        ctx.outCnt++;
        if (ret)
            return ret;
    }
    return ctx;
}

const CSV = 'csv';

const OUTPUTS = {
    [CSV]: writeOutCsv
}

function writeOutCsv (ctx) {
    let fields = Options.csv.value.split(',');
    let valuesIt = [];

    for (const path of fields) {
        valuesIt.push({
            pathIt: pathIterator(ctx.out, path)
        });
    }

    if (Options.ml.value) {
        log('Matches: ' + ctx.matches);
    }

    return next();

    function next () {
        if (!ctx.header) {
            ctx.header = true;
            return writeHeader().then(next);
        }

        let more = false;
        for (const valueIt of valuesIt) {
            if (!valueIt.done) {
                const it = valueIt.pathIt.next();
                valueIt.done = it.done;
                if (!it.done) {
                    valueIt.data = it.value.data;
                    valueIt.path = it.value.path;
                    more = true;
                }
            }
        }
        if (!more)
            return Promise.resolve(ctx);
        
        return writeLine().then(next);
    }

    function writeHeader() {
        let header = '';
        for (const field of fields) {
            if (header.length > 0)
                header += Options.sep.value;
            header += Options.del.value +
                        field +
                        Options.del.value;
        }
        if (ctx.outf) {
            return ctx.outf.write(header + '\n');
        } else {
            console.log(header);
        }
        return Promise.resolve();
    }

    function writeLine () {
        let line = '';
        let output = !Options.ml.value;
        
        for (const valueIt of valuesIt) {
            const data = valueIt.data;
            if (line.length > 0)
                line += Options.sep.value;
            if (typeof data !== 'undefined' &&
                data !== null) {
                let del = typeof data !== 'number' ?
                    Options.del.value : '';
                line += del + data + del;
            } else {
                line += Options.nil.value;
            }

            if (Options.ml.value) {
                if (ctx.matches.indexOf(valueIt.path) >= 0)
                    output = true;
            }
        }

        if (output) {
            if (ctx.outf) {
                return ctx.outf.write(line + '\n')
            } else {
                console.log(line);
            }
        }

        return Promise.resolve();
    }
}

const PROGRESS_INTERVAL = 500;

function logProgress (ctx) {
    let now = new Date().getTime();

    if (!ctx.lastProgress || now - ctx.lastProgress >= PROGRESS_INTERVAL || ctx.finish) {
        ctx.lastProgress = now;

        let progress = ctx.inpos * 100 / ctx.infStats.size;
        process.stdout.write('\r' + progress.toFixed(1) + ' %');

        const steps = 25;
        process.stdout.write(' [');
        for (let step = 1; step <= steps; step++) {
            let threshold = step * (100 / steps);
            if (progress >= threshold) {
                process.stdout.write('=');
            } else {
                process.stdout.write(' ');
            }
        }
        process.stdout.write(']');

        if (ctx.lastInpos) {
            let bytesPerIval = ctx.inpos - ctx.lastInpos;

            ctx.bytesPerIvalAvg += bytesPerIval;
            ctx.bytesPerIvalCnt++;
            let bytesPerIvalAvg = ctx.bytesPerIvalAvg / ctx.bytesPerIvalCnt;
            if (ctx.bytesPerAvgCnt >= 10) {
                ctx.bytesPerAvg = bytesPerIvalAvg * 5;
                ctx.bytesPerAvgCnt = 5;
            }

            let bytesPerSec = bytesPerIvalAvg * 1000 / PROGRESS_INTERVAL;
            let bytesRemaining = ctx.infStats.size - ctx.inpos;
            let etaSeconds = bytesRemaining / bytesPerSec;
            let eta = new Date(etaSeconds * 1000).toISOString().substr(11, 8);
            process.stdout.write(' ETA ' + eta);

        } else {
            ctx.bytesPerIvalAvg = 0;
            ctx.bytesPerIvalCnt = 0;
        }

        process.stdout.write(
            (ctx.rules.length > 0 ? ' (Matches ' : ' (Total ') + ctx.outCnt + ')');

        ctx.lastInpos = ctx.inpos;
    }

    return ctx;
}

function finish (ctx) {
    console.log("\nFinished!");
    console.log("Records: " + ctx.inCnt);
    if (ctx.trailler) {
        console.log("Trailler: ");
        for (let property of ctx.trailler) {
            const value = ctx.trailler[property];
            console.log(property + ': ' + value);
        }
    }
}

function help () {
    console.log('./cnpj ops (args) -opt (opt_args)');

    console.log('\nOperations');
    console.log('\thelp: Show this help');
    console.log('\tparse: Parse batch import file');

    console.log('\nOptions');
    for (let property in Options) {
        let option = Options[property];
        console.log('\t' + property + ': (' + option.type + ') ' + option.description);
    }
}

parseCmdline()
    .then(exec)
    .catch((error) => {

    if (error instanceof Error)
        console.log(error);
    else 
        console.log('\nError: ' + error);

    process.exit(1);
});
