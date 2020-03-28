import React, {useEffect, useState} from 'react';
import './App.scss';
import {range} from 'rxjs';
import {map, toArray, bufferCount} from 'rxjs/operators'
import QuizView from "./app/QuizView";
import StrategyView from "./strategy/StrategyView";
import {Strategy} from "./strategy/Strategy";
import {generateQuiz} from "./strategy/quizGenerator";

export interface Quiz {
    num1?: number,
    operator: string,
    num2?: number,
    num3?: number,
}


function App() {
    const [problems, setProblems] = useState<Array<Array<Quiz>>>([]);
    const defSt : Strategy = {
        levelUp: true,
        space: 2,
        operator: "+",
        max: 100,
        min: 10,
    };
    const [strategy, setStrategy] = useState(defSt);

    useEffect(() => {
        range(0, 100)
            .pipe(
                map(() => {
                    return generateQuiz(strategy)
                }),
                bufferCount(7),
                toArray(),
                // windowCount(100),
            )
            .subscribe(value => {
                setProblems(value);
            })
    }, [strategy]);

    const cube = problems.map((row,i) => {
        return <QuizView key={i} problems={row}/>
    });

    return (
        <div>
            <StrategyView value={strategy} onChange={setStrategy}/>
            {cube}
        </div>
    );
}

export default App;
