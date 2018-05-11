from flask import Flask, jsonify, Response
from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError
import logging
from logging.handlers import RotatingFileHandler

#define driver to cnnecto to neo4j
driver = GraphDatabase.driver("bolt://localhost",
                             auth=basic_auth("neo4j","neo4j_recommendation_system"),
                             encrypted=False,
                             trust=TRUST_ON_FIRST_USE)

# define the queries to interact with neo4j
query = {}

query['match_person'] = """
match (p:Person{person_id:{person_id}})
return p"""

query['match_tag'] = """
match (t:Tag{tag_id:{tag_id}})
return t"""

query['match_project'] = """
match (p:Project{project_id:{project_id}})
return p"""

query['create_person'] = """
merge (p:Person{person_id:{person_id}})
"""

query['create_tag'] = """
merge (t:Tag{tag_id:{tag_id}})
"""

query['create_project'] = """
merge (p:Project{project_id:{project_id}})
"""

query['create_project_has_tag'] = """
match (p:Project{project_id:{project_id}})
match (t:Tag{tag_id:{tag_id}})
merge (p)-[:has_tag]->(t)
"""

query['create_person_has_tag'] = """
match (p:Person{person_id:{person_id}})
match (t:Tag{tag_id:{tag_id}})
merge (p)-[:has_tag]->(t)
"""

query['delete_person'] =  """
match (p:Person{person_id:{person_id}})
detach delete (p)
"""

query['delete_tag'] = """
match (t:Tag{tag_id:{tag_id}})
detach delete (t)
"""

query['delete_project'] = """
match (p:Project{project_id:{project_id}})
detach delete (p)
"""

query['delete_person_has_tag'] = """
match (p:Person{person_id:{person_id}})-[res:has_tag]->(t:Tag{tag_id:{tag_id}})
delete res
"""

query['delete_project_has_tag'] = """
match (p:Project{project_id:{project_id}})-[res:has_tag]->(t:Tag{tag_id:{tag_id}})
delete res
"""

query['create_person_member_of_project'] = """
match (p:Project{project_id:{project_id}})
match (ps:Person{person_id:{person_id}})
merge (ps)-[:member_of]->(p)
"""

query['delete_person_member_of_project'] = """
match (p:Project{project_id:{project_id}})<-[res:member_of]-(ps:Person{person_id:{person_id}})
delete res
"""

query['get_recommendation'] = """
MATCH (p:Project{project_id:{project_id}})-[:has_tag]->(t)<-[:has_tag]-(ps:Person)
WHERE NOT (ps)-[:member_of]->(p)
RETURN ps.person_id, COUNT(t) AS tagsInCommon,
COLLECT(t.tag_id) AS topics
ORDER BY tagsInCommon DESC
LIMIT {num_of_recommendation}
"""


#Continuously listen to the connection and print messages as recieved
app = Flask(__name__)
@app.route('/log', methods = ["GET","POST"] )
def flask_service():
    app.logger.warning('A warning occurred (%d apples)', 42)
    app.logger.error('An error occurred')
    app.logger.info('Info')
    return "foo"

@app.route('/create/person/<person_id>',methods = ["POST"])
def create_person(person_id):
    person_id = str(person_id)

    try:
        session = driver.session()
        result = session.run(query['create_person'],person_id = person_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, person_id=person_id)
        else:
            return jsonify(response=False, person_id=person_id)

    return jsonify(response=True, person_id=person_id,action = 'create')

@app.route('/create/tag/<tag_id>',methods = ["POST"] )
def create_tag(tag_id):
    tag_id = str(tag_id)

    try:
        session = driver.session()
        result = session.run(query['create_tag'],tag_id = tag_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, tag_id=tag_id)
        else:
            return jsonify(response=False, tag_id=tag_id)

    return jsonify(response=True, tag_id=tag_id,action= 'create')

@app.route('/create/project/<project_id>',methods = ["POST"])
def create_project(project_id):
    project_id= str(project_id)

    try:
        session = driver.session()
        result = session.run(query['create_project'],project_id = project_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, project_id=project_id)
        else:
            return jsonify(response=False, project_id=project_id)

    return jsonify(response=True, project_id=project_id,action = 'create')

@app.route('/create/project_has_tag/<project_id>&&<tag_id>',methods = ["POST"])
def create_project_has_tag(project_id,tag_id):
    project_id = str(project_id)
    tag_id = str(tag_id)

    try:
        session = driver.session()
        project = session.run(query['match_project'],project_id = project_id)
        if not project.peek():
            return jsonify(response= False, info="project_id " +project_id + " does not exist")
        tag = session.run(query['match_tag'], tag_id=tag_id)
        if not tag.peek():
            return jsonify(response=False, info="tag_id" + tag_id + " does not exist ")
        result = session.run(query['create_project_has_tag'],project_id = project_id,tag_id = tag_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, project_id = project_id, tag_id=tag_id)
        else:
            return jsonify(response=False, project_id = project_id, tag_id=tag_id)

    return jsonify(response=True,  project_id = project_id, tag_id=tag_id,action='create')

@app.route('/create/person_has_tag/<person_id>&&<tag_id>',methods = ["POST"])
def create_person_has_tag(person_id,tag_id):
    person_id = str(person_id)
    tag_id = str(tag_id)

    try:
        session = driver.session()
        person = session.run(query['match_person'], person_id=person_id)
        if not person.peek():
            return jsonify(response=False, info="person_id " + person_id + " does not exist")
        tag = session.run(query['match_tag'], tag_id=tag_id)
        if not tag.peek():
            return jsonify(response=False, info="tag_id" + tag_id + " does not exist")
        result = session.run(query['create_person_has_tag'], person_id=person_id, tag_id=tag_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, person_id=person_id, tag_id=tag_id)
        else:
            return jsonify(response=False, person_id=person_id, tag_id=tag_id)

    return jsonify(response=True, person_id=person_id, tag_id=tag_id, action='create')

@app.route('/delete/person/<person_id>',methods = ["POST"])
def delete_person(person_id):
    person_id = str(person_id)

    try:
        session = driver.session()
        result = session.run(query['delete_person'],person_id = person_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, person_id=person_id)
        else:
            return jsonify(response=False, person_id=person_id)

    return jsonify(response=True, person_id=person_id,action = 'delete')

@app.route('/delete/tag/<tag_id>',methods = ["POST"])
def delete_tag(tag_id):
    tag_id = str(tag_id)

    try:
        session = driver.session()
        result = session.run(query['delete_tag'],tag_id = tag_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, tag_id=tag_id)
        else:
            return jsonify(response=False, tag_id=tag_id)

    return jsonify(response=True, tag_id=tag_id, action = 'delete')

@app.route('/delete/project/<project_id>',methods = ["POST"])
def delete_project(project_id):
    project_id = str(project_id)

    try:
        session = driver.session()
        result = session.run(query['delete_project'],project_id = project_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, project_id=project_id)
        else:
            return jsonify(response=False, project_id=project_id)

    return jsonify(response=True, project_id=project_id, action = 'delete')

@app.route('/delete/person_has_tag/<person_id>&&<tag_id>',methods = ["POST"])
def delete_person_has_tag(person_id,tag_id):
    person_id = person_id
    tag_id = tag_id

    try:
        session = driver.session()
        person = session.run(query['match_person'], person_id=person_id)
        if not person.peek():
            return jsonify(response=False, info="person_id " + person_id + " does not exist")
        tag = session.run(query['match_tag'], tag_id=tag_id)
        if not tag.peek():
            return jsonify(response=False, info="tag_id" + tag_id + " does not exist")
        result = session.run(query['delete_person_has_tag'],person_id = person_id, tag_id = tag_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, person_id=person_id, tag_id=tag_id)
        else:
            return jsonify(response=False, person_id=person_id, tag_id=tag_id)

    return jsonify(response=True, person_id=person_id, tag_id=tag_id, action = 'delete')

@app.route('/delete/project_has_tag/<project_id>&&<tag_id>',methods = ["POST"])
def delete_project_has_tag(project_id,tag_id):
    project_id = project_id
    tag_id = tag_id
    try:
        session = driver.session()
        project = session.run(query['match_project'], project_id=project_id)
        if not project.peek():
            return jsonify(response=False, info="project_id " + project_id + " does not exist")
        tag = session.run(query['match_tag'], tag_id=tag_id)
        if not tag.peek():
            return jsonify(response=False, info="tag_id" + tag_id + " does not exist ")
        result = session.run(query['delete_project_has_tag'],project_id = project_id, tag_id = tag_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, project_id=project_id, tag_id=tag_id)
        else:
            return jsonify(response=False, project_id=project_id, tag_id=tag_id)

    return jsonify(response=True, project_id=project_id, tag_id=tag_id, action = 'delete')

@app.route('/create/person_member_of_project/<person_id>&&<project_id>',methods = ["POST"])
def create_person_member_of_project(person_id,project_id):
    project_id = project_id
    person_id = person_id
    try:
        session = driver.session()
        project = session.run(query['match_project'], project_id=project_id)
        if not project.peek():
            return jsonify(response=False, info="project_id " + project_id + " does not exist")
        person = session.run(query['match_person'], person_id=person_id)
        if not person.peek():
            return jsonify(response=False, info="person_id" + person_id + " does not exist ")
        result = session.run(query['create_person_member_of_project'],project_id = project_id, person_id = person_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, project_id=project_id, person_id=person_id)
        else:
            return jsonify(response=False, project_id=project_id, person_id=person_id)

    return jsonify(response=True, project_id=project_id, person_id=person_id, action = 'create')

@app.route('/delete/person_member_of_project/<person_id>&&<project_id>',methods = ["POST"])
def delete_person_member_of_project(person_id,project_id):
    project_id = project_id
    person_id = person_id
    try:
        session = driver.session()
        project = session.run(query['match_project'], project_id=project_id)
        if not project.peek():
            return jsonify(response=False, info="project_id " + project_id + " does not exist")
        person = session.run(query['match_person'], person_id=person_id)
        if not person.peek():
            return jsonify(response=False, info="person_id" + person_id + " does not exist ")
        result = session.run(query['delete_person_member_of_project'],project_id = project_id, person_id = person_id)
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, project_id=project_id, person_id=person_id)
        else:
            return jsonify(response=False, project_id=project_id, person_id=person_id)

    return jsonify(response=True, project_id=project_id, person_id=person_id, action = 'delete')

@app.route('/get/recommendation/<project_id>&&<num_of_recommendation>')
def get_recommendation(project_id,num_of_recommendation=50):
    project_id = str(project_id)
    num_of_recommendation = int(num_of_recommendation)

    try:
        session = driver.session()
        project = session.run(query['match_project'], project_id=project_id)
        if len(project.keys()) == 0:
            return jsonify(response=False, info="project_id " + project_id + " does not exist")
        result = session.run(query['get_recommendation'],project_id = project_id, num_of_recommendation = num_of_recommendation)
        res = [{i.values()[0]: [i.values()[1],i.values()[2]]} for i in result.records()]
        session.close()

    except Exception as e:

        print('*** Got exception', e)
        if not isinstance(e, CypherError):
            session.rollback()
            return jsonify(response=False, project_id=project_id)
        else:
            return jsonify(response=False, project_id=project_id)

    return jsonify(response=True, project_id=project_id, res = res)

if __name__ == '__main__':
    handler = RotatingFileHandler('info.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)

    app.run(host='localhost', debug=True)
