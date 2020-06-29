import ast

import psycopg2 as psycopg2
import json
import sys


# checks if ID already exists
def checkIDtaken(id):
    cur.execute(f"""
    SELECT * FROM takenid
    WHERE takenid.id = {id};""")
    conn.commit()

    s = cur.fetchall()
    if not s:
        return False
    return True


def addUniqueID(id):
    cur.execute(f"""

       INSERT INTO takenid (id)
       VALUES (
         {id}  
       );""")

    conn.commit()


def checkCredentials(id, pswrd):
    cur.execute(f"""
    SELECT * FROM member
    WHERE member.id = {id} AND (password = crypt('{pswrd}', password));
    """)
    conn.commit()

    s = cur.fetchall()
    if not s:
        print(json.dumps(ast.literal_eval('{"status" : "INVALID CREDENTIALS"}')))
        return False

    return True


def checkFrozen(timestamp, memberid):
    cur.execute(f'SELECT lastactive FROM member WHERE member.id = {memberid};')

    conn.commit()
    res = cur.fetchall()
    clientTimestamp = res[0][0]

    return (timestamp - clientTimestamp) > (362.5 * 24 * 60 * 60 * 1000)


def updateTimestamp(timestamp, memberID):
    cur.execute(f"""
            UPDATE member
            SET lastactive = {timestamp}
            WHERE id = {memberID}; 
            """)


def createProject(projectID, authority):
    cur.execute(f"""
    SELECT * FROM project
    WHERE project.id = {projectID}
    """)

    if cur.fetchall():
        return
    if checkIDtaken(projectID):
        print(json.dumps(ast.literal_eval('{"status" : "COULD NOT CREATE PROJECT - ID ALREADY TAKEN"}')))
        return

    addUniqueID(projectID)

    cur.execute(f"""

        INSERT INTO project (id, authorityid)
        VALUES (
          {projectID},{authority}  
        );""")

    conn.commit()


# leader <timestamp> <password> <member>
def addMember(js, isLeader):
    name = list(js.keys())[0]
    timestamp, password, memberid = js[name]["timestamp"], js[name]["password"], js[name]["member"]

    if checkIDtaken(memberid):
        print(json.dumps(ast.literal_eval('{"status" : "COULD NOT CREATE MEMBER - ID ALREADY TAKEN"}')))
        return

    addUniqueID(memberid)

    cur.execute(f"""

    INSERT INTO member (id,password,lastactive,leader,upvotes,downvotes)
    VALUES (
            {memberid}, '{password}',  {timestamp}, {isLeader}, {0},  {0})
    ;""")

    cur.execute(f"""

    UPDATE member SET password = crypt('{password}', gen_salt('bf'))
    WHERE member.id = {memberid};

""")

    conn.commit()
    print(json.dumps(ast.literal_eval('{"status" : "OK"}')))


def getProjectAuthority(projectID):
    cur.execute(f"""
        SELECT authorityid FROM project
        WHERE project.id = {projectID};
    """)
    # out of range if project does not exist
    return cur.fetchall()[0][0]


# protest <timestamp> <member> <password> <action> <project> [ <authority> ]
def addSuport(js):
    timestamp, memberid, password, action, project \
        = js["support"]["timestamp"], js["support"]["member"], js["support"]["password"], js["support"]["action"], \
          js["support"]["project"]

    if not checkIDtaken(memberid):
        addMember(js, False)

    if not checkCredentials(memberid, password):
        return

    if checkFrozen(timestamp, memberid):
        print(json.dumps(ast.literal_eval('{"status" : "MEMBER IS FROZEN"}')))
        return
    updateTimestamp(timestamp, memberid)

    if checkIDtaken(action):
        print(json.dumps(ast.literal_eval('{"status" : "COULD NOT CREATE SUPPORT - ID ALREADY TAKEN"}')))
        return

    if "authority" in js["support"]:
        authority = js["support"]["authority"]
        createProject(project, authority)

    addUniqueID(action)

    cur.execute(f"""

    INSERT INTO action (id,projectid,memberid,authorityid,type,creationdate)
    VALUES (
            {action},{project},{memberid},{getProjectAuthority(project)},'support',{timestamp});""")

    conn.commit()
    print(json.dumps(ast.literal_eval('{"status" : "OK"}')))


def addProtest(js):
    timestamp, memberid, password, action, project \
        = js["protest"]["timestamp"], js["protest"]["member"], js["protest"]["password"], js["protest"]["action"], \
          js["protest"]["project"]

    if ("authority" in js["protest"]):
        authority = js["protest"]["authority"]

    if not checkIDtaken(memberid):
        addMember(js, False)

    if not checkCredentials(memberid, password):
        return

    if checkFrozen(timestamp, memberid):
        print(json.dumps(ast.literal_eval('{"status" : "MEMBER IS FROZEN"}')))
        return
    updateTimestamp(timestamp, memberid)
    createProject(project, authority)

    if checkIDtaken(action):
        print(json.dumps(ast.literal_eval('{"status" : "COULD NOT CREATE PROTEST - ID ALREADY TAKEN"}')))
        return

    addUniqueID(action)

    cur.execute(f"""

    INSERT INTO action (id,projectid,memberid,authorityid,type,creationdate)
    VALUES ( {action},{project},{memberid},{getProjectAuthority(project)},'protest',{timestamp});""")

    conn.commit()
    print(json.dumps(ast.literal_eval('{"status" : "OK"}')))


# upvote <timestamp> <member> <password> <action>
# votes : actionid, memberid, type
def vote(js, type):
    name = list(js.keys())[0]
    timestamp, member, password, action = js[name]["timestamp"], js[name]["member"], js[name]["password"], \
                                          js[name]["action"]
    if not checkIDtaken(member):
        addMember(js, False)

    if not checkCredentials(member, password):
        return

    if checkFrozen(timestamp, member):
        print(json.dumps(ast.literal_eval('{"status" : "MEMBER IS FROZEN"}')))
        return

    updateTimestamp(timestamp, member)

    if not checkCredentials(member, password):
        return

    cur.execute(f"""
        SELECT * FROM votes
        WHERE votes.memberid = {member} and {action} = votes.actionid;
    """)
    if cur.fetchall():
        print(json.dumps(ast.literal_eval('{"status" : "ALREADY VOTED"}')))
        return

    cur.execute(f"""
    
    INSERT INTO votes(actionid, memberid, type)
    VALUES({action},{member},'{type}');
    """)

    if type == "upvote":
        cur.execute(f"""
        UPDATE action
        SET upvotes = upvotes + 1
        WHERE action.id = {action}; 
        """)
        cur.execute(f"""
                UPDATE member
                SET upvotes = member.upvotes + 1
                FROM action
                WHERE action.memberid = member.id AND action.id = {action}; 
                """)


    else:
        cur.execute(f"""
                UPDATE action
                SET downvotes = downvotes + 1
                WHERE action.id = {action}; 
                """)
        cur.execute(f"""
                      UPDATE member
                      SET downvotes = member.downvotes + 1
                      FROM action
                      WHERE action.memberid = member.id AND action.id = {action}; 
                      """)

    conn.commit()
    print(json.dumps(ast.literal_eval('{"status" : "OK"}')))


# votes <timestamp> <member> <password> [ <action> | <project> ]
def votes(js):
    timestamp, member, password = js["votes"]["timestamp"], js["votes"]["member"], js["votes"]["password"]

    if "action" in js["votes"]:
        type = js["actions"]["action"]

        cur.execute(f"""
            SELECT
        member.id,
        COUNT(type) filter (where type = 'upvote' and memberid = member.id and votes.actionid = {type} ) AS num_upvotes,
        COUNT(type) filter (where type = 'downvote' and memberid = member.id and votes.actionid = {type}) AS num_downvotes
        FROM
        votes,member
        GROUP BY member.id
        ORDER BY
        member.id
        """)
    #
    # elif "project" in js["votes"]:
    #     type = js["actions"]["project"]
    #
    #     cur.execute(f"""
    #                 SELECT
    #             member.id,
    #             COUNT(type) filter (where type = 'upvote' and memberid = member.id and votes.projectid = {type} ) AS num_upvotes,
    #             COUNT(type) filter (where type = 'downvote' and memberid = member.id and votes.projectid = {type}) AS num_downvotes
    #             FROM
    #             votes,member
    #             GROUP BY member.id
    #             ORDER BY
    #             member.id
    #             """)

    else:
        cur.execute("""
    SELECT
member.id,
COUNT(type) filter (where type = 'upvote' and memberid = member.id) AS num_upvotes,
COUNT(type) filter (where type = 'downvote' and memberid = member.id) AS num_downvotes
FROM
votes,member
GROUP BY member.id
ORDER BY
member.id
""")

    conn.commit()

    print(json.dumps(ast.literal_eval('{"status" : "OK" , "data" : ' + f'{cur.fetchall()}' + "}")))


# trolls <timestamp>
def trolls(js):
    timestamp = js["trolls"]["timestamp"]

    cur.execute(f"""
                   SELECT id FROM member
                   ORDER BY (downvotes - upvotes) DESC, id ASC;
                   """)

    print(json.dumps(ast.literal_eval('{"status" : "OK" , "data" : ' + f'{cur.fetchall()}' + "}")))


# projects <timestamp> <member> <password>
def projects(js):
    timestamp, member, password = js["projects"]["timestamp"], js["projects"]["member"], js["projects"]["password"]

    if checkFrozen(timestamp, member):
        print(json.dumps(ast.literal_eval('{"status" : "MEMBER IS FROZEN"}')))
        return

    updateTimestamp(timestamp, member)

    if "authority" in js["actions"]:
        authority = js["actions"]["authority"]
        cur.execute(f"""
                                   SELECT authorityid,id FROM project
                                    WHERE authorityid = {authority};
                                   """)

    else:

        cur.execute(f"""
                           SELECT authorityid,id FROM project
                           GROUP BY authorityid;
                           """)
    conn.commit()


# actions <timestamp> <member> <password> [ <type> ] [ <project> | <authority> ]
def actions(js):
    timestamp, member, password = js["actions"]["timestamp"], js["actions"]["member"], js["actions"]["password"]

    if checkFrozen(timestamp, member):
        print(json.dumps(ast.literal_eval('{"status" : "MEMBER IS FROZEN"}')))
        return

    updateTimestamp(timestamp, member)

    if "type" in js["actions"]:
        type = js["actions"]["type"]
        cur.execute(f"""
                                  SELECT * FROM action
                                  WHERE action.type = {type}
                                  GROUP BY authorityid
                                  ORDER BY  action.id;""")
    elif "project" in js["actions"]:
        type = js["actions"]["project"]
        cur.execute(f"""
                                      SELECT * FROM action
                                      WHERE action.projectid = {type}
                                      GROUP BY authorityid
                                      ORDER BY  action.id;""")
    elif "authority" in js["actions"]:
        type = js["actions"]["authority"]
        cur.execute(f"""
                                      SELECT * FROM action
                                      WHERE action.authorityid = {type}
                                      GROUP BY authorityid
                                      ORDER BY  action.id;""")

    else:
        cur.execute(f"""
                          SELECT * FROM action
                          GROUP BY authorityid
                          ORDER BY  action.id;""")

    conn.commit()


# { "open": { "database": "student", "login": "app", "password": "qwerty"}}
def requestOpen(js):
    dbname, username, passwrd = js["open"]["database"], js["open"]["login"], js["open"]["password"]
    global conn
    conn = psycopg2.connect(database=dbname,
                            user=username,
                            password=passwrd,
                            host="localhost",
                            port="5432")
    global cur
    cur = conn.cursor()
    print(json.dumps(ast.literal_eval('{"status" : "OK"}')))


def executeQueries():
    while True:
        s = input()
        if s == "":
            break

        querry = json.loads(s)

        if 'votes' in querry:
            votes(querry)  # DONE
        elif 'upvote' in querry:
            vote(querry, "upvote")  # DONE
        elif 'downvote' in querry:
            vote(querry, "downvote")
        elif 'support' in querry:  # DONE
            addSuport(querry)  # DONE
        elif 'projects' in querry:
            projects(querry)  # DONE
        elif 'protest' in querry:
            addProtest(querry)  # DONE
        elif 'actions' in querry:
            actions(querry)
        elif 'trolls' in querry:
            trolls(querry)  # DONE
        elif 'leader' in querry:
            addMember(querry, True)  # DONE
        elif 'open' in querry:
            requestOpen(querry)


if len(sys.argv) == 2:
    if sys.argv[1] == "--init":
        # START OF INITinput())

        requestOpen(json.loads(input()))

        SQL_INIT = open('init', 'r')

        cur.execute(SQL_INIT.read())

        conn.commit()
        ##END OF INIT

executeQueries()

#    cur = conn.cursor()
#    cur.execute("""
#       SELECT * FROM member""");

#    conn.commit()
#    print(cur.fetchall())

