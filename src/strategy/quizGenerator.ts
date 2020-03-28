import {Strategy} from "./Strategy";
import {Quiz} from "../App";

const randomInt = (max: number, min: number = 0) => {
    return parseInt(String(Math.random() * (max - min))) + min;
};

export function generateQuiz(strategy: Strategy): Quiz {
    while (true) {
        let first = randomInt(strategy.max, strategy.min);
        let second = randomInt(strategy.max, strategy.min);

        // 检查最大值
        if (first + second > strategy.max) {
            continue;
        }

        // 检查进位
        if (strategy.levelUp && first % 10 + second % 10 < 10) {
            continue;
        }

        // 加减法
        const operator: any = strategy.operator === 'random' ? ["+", "-"][randomInt(100) % 2] : strategy.operator;
        let row: any = [first, second, first + second];
        if (operator === '-') {
            const [a, b, c] = row;
            row = [c, b, a];
        }

        row[strategy.space] = null;

        return {
            num1: row[0],
            num2: row[1],
            num3: row[2],
            operator: operator,
        }
    }
}