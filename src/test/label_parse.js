const fs = require('fs');
const rxjs = require('rxjs');
const op = require('rxjs/operators');
const YAML = require('yaml');

const blackList = [
    "com.sankuai.ep.platform.quake",
    "quake-agent",
    "test",
    "org.testng.TestNg",
    "unknownService",
];
async function run() {
    let buffer = fs.readFileSync('./labels.json');

    let str = buffer.toString('utf-8');

    let json = JSON.parse(str);

    // console.log(JSON.stringify(json, null, 2));

    let resources = await rxjs.of(json)
        .pipe(
            op.flatMap(res => {
                return res.content.resourceList;
            }),
            op.flatMap(apply => {
                if (apply.status != 1) {
                    return [];
                }

                const appkey = apply.authIds;
                if (blackList.indexOf(appkey) >= 0) {
                    return [];
                }
                const labelStr = apply.resourceContent;
                // 解析标签
                const labels = labelStr.split('\n');
                return labels.filter(l => {
                    return l.indexOf('[子资源]') >= 0;
                }).map(l => {
                    const label = l.toString().match(/.+子资源.:(.+)\[\d+\]/)[1];
                    return {
                        label: label,
                        appkey: appkey,
                    }
                })
            }),
            op.groupBy(a => a.appkey),
            op.mergeMap((values) => {
                const key = values.key;
                return values.pipe(
                    op.toArray(),
                    op.map(arr => {
                        const labels = arr.map(a => a.label);
                        let uniqLabels = [...new Set(labels)];
                        return {
                            appkey: key,
                            count: uniqLabels.length,
                            labels: uniqLabels,
                        }
                    })
                )
            }),
            op.toArray(),
        )
        .toPromise();

    resources.sort((a, b) => {
        return -(a.labels.length - b.labels.length);
    })
    let yaml = YAML.stringify(resources);
    console.log(yaml);

    console.log(resources.length);
}


run();