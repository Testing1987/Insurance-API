from enum import Enum
from os import getenv
from typing import List, Optional

import strawberry
from fastapi import FastAPI, Request
from neo4j import GraphDatabase
from strawberry.asgi import GraphQL
from dotenv import load_dotenv
load_dotenv()

AURADB_URI = getenv("AURADB_URI")
AURADB_USERNAME = getenv("AURADB_USERNAME")
AURADB_PASSWORD = getenv("AURADB_PASSWORD")


def _and(cur_filter, new_filter):
    if not cur_filter:
        return f"WHERE {new_filter} "
    return f"AND {new_filter} "


@strawberry.enum
class AnswerType(Enum):
    string = "String"
    bool = "Bool"
    float = "Float"
    # Unsure of the status of these - 2022-11-20:
    int = "Int"
    list = "List"
    label = "Label"
    date = "Date"


@strawberry.type
class Answer:
    uuid: str
    answer: str
    type: AnswerType
    createdAt: str
    updatedAt: str

    @classmethod
    def marshal(cls, obj) -> "Answer":
        return cls(
            uuid=obj['uuid'],
            answer=obj['answer'],
            type=obj['type'],
            createdAt=obj['created_at'],
            updatedAt=obj['updated_at']
        )


@strawberry.input
class SaveAnswerInput:
    answer: str
    questionUuid: str

    # TODO: Potentially refactor away from using this,
    # because the backend could look these up - 2022-11-20:
    type: AnswerType

    def to_dict(self):
        return {
            "answer": self.answer,
            "type": self.type.value,
            "questionUuid": self.questionUuid,
        }


@strawberry.input
class SaveAnswersInput:
    applicationUuid: str
    answers: List[SaveAnswerInput]

    def serialize(self):
        return [answer.to_dict() for answer in self.answers]


@strawberry.type
class Question:
    uuid: str
    sectionUuid: str
    order: int
    type: str
    questionString: str
    answer: Optional[Answer]

    @classmethod
    def marshal(cls, question, answer=None) -> "Question":
        answer = answer if answer is None else Answer.marshal(answer)
        return cls(
            uuid=question['uuid'],
            sectionUuid=question['section_uuid'],
            order=question['order'],
            type=question['type'],
            questionString=question['question_string'],
            answer=answer
        )


@strawberry.type
class Application:
    uuid: str
    name: str
    version: str
    createdAt: str
    updatedAt: str
    questions: List[Question]

    @classmethod
    def marshal(cls, application) -> "Application":

        questions = application.get('questions')
        questions = questions if questions else []
        print("questions", questions)

        return cls(
            uuid=application['uuid'],
            name=application['name'],
            version=application['version'],
            createdAt=application['created_at'],
            updatedAt=application['updated_at'],
            questions=[
                Question.marshal(
                    question.get('question'), question.get('answer')
                )
                for question in questions
            ]
        )


@strawberry.type
class Query:
    @strawberry.field()
    def applications(self) -> List[Application]:
        with graph.session() as session:
            query = """
                MATCH (a:Application) RETURN a
            """
            rows = session.run(query).data()
        return [
            Application.marshal(
                row['a']
            ) for row in rows
        ]

    @strawberry.field()
    def getApplicationWithQuestion(
        self,
        applicationUuid: str
    ) -> Application:

        ans_query = f"""
            MATCH
                (app:Application {{uuid: '{applicationUuid}'}})-->
                (q:Question)-[qa:HAS_ANSWER
                    {{has_application_uuid: '{applicationUuid}'}}
                ]->(ans:Answer)
            RETURN app, q, qa, ans
        """

        no_ans_query = f"""
            MATCH
                (app:Application {{uuid: '{applicationUuid}'}})-->
                (q:Question)
            WHERE NOT
                (q)-[:HAS_ANSWER
                    {{has_application_uuid: '{applicationUuid}'}}
                ]->()
            RETURN app, q
        """
        with graph.session() as session:
            ans_rows = list()
            for record in session.run(ans_query):
                row = dict(record)
                ans_rows.append(row)
            no_ans_rows = list()
            for record in session.run(no_ans_query):
                row = dict(record)
                no_ans_rows.append(row)

        if ans_rows:
            app = dict(ans_rows[0]['app'])
            app['questions'] = list()
        if no_ans_rows:
            app = dict(no_ans_rows[0]['app'])
            app['questions'] = list()

        for row in ans_rows:
            app['questions'].append(
                {
                    "question": dict(row['q']),
                    "answer": dict(row['ans'])
                }
            )
        for row in no_ans_rows:
            app['questions'].append({"question": dict(row['q'])})
        return Application.marshal(app)


@strawberry.type
class Mutation:
    @strawberry.mutation()
    def saveAnswers(
        self, data: SaveAnswersInput
    ) -> Optional[Question]:

        applicationUuid = data.applicationUuid

        with graph.session() as session:
            query = f"""
                UNWIND $answers as answer
                MATCH (q:Question {{uuid: answer.questionUuid}})
                MERGE
                    (q)-[r:HAS_ANSWER {{
                        has_application_uuid: '{applicationUuid}'
                    }}]->(a:Answer)
                ON CREATE SET
                    a.uuid = apoc.create.uuid(),
                    a.type = answer.type,
                    a.answer = answer.answer,
                    a.created_at = datetime({{epochmillis:timestamp()}}),
                    a.updated_at = datetime({{epochmillis:timestamp()}}),
                    r.uuid = apoc.create.uuid(),
                    r.has_application_uuid = '{applicationUuid}',
                    r.created_at = datetime({{epochmillis:timestamp()}}),
                    r.updated_at = datetime({{epochmillis:timestamp()}})
                ON MATCH SET
                    a.type = answer.type,
                    a.answer = answer.answer,
                    a.updated_at = datetime({{epochmillis:timestamp()}}),
                    r.updated_at = datetime({{epochmillis:timestamp()}})
                RETURN q, a, r
            """
            session.run(query, answers=data.serialize())
        return None


schema = strawberry.Schema(query=Query, mutation=Mutation)
graphql_app = GraphQL(schema)
graph = GraphDatabase.driver(
    AURADB_URI, auth=(AURADB_USERNAME, AURADB_PASSWORD)
)

app = FastAPI()


@app.middleware("http")
def my_middleware(request: Request, call_next):
    response = call_next(request)
    return response


app.add_route("/graphql", graphql_app)
graph.close()
