from py2neo import Graph,Node,Relationship
import time,sys
if len(sys.argv) != 2:
    print("wrong argv")
    exit(33)
#query_name = sys.argv[1]
k=int(sys.argv[1])
print("k="+str(k))
test_graph = Graph(
    "http://localhost:7474", 
    username="neo4j", 
    password="a74300"
)
time_start=time.time()
query_name = "dbpediaL53046.edge"
if True:
    #filename = query_name+str(k)+".true"
    
    f_true_out = open(query_name+str(k)+".true_runtime", "w")
    f_false_out = open(query_name+str(k)+".false_runtime","w")
    
    f_true = open(query_name+str(k)+".true", "r")
    f_false = open(query_name+str(k)+".false","r")
    query_num = 0
    for i in f_true.readlines():
        time_s2=time.time()
        query_num +=1
        print(query_num)
        line = i.split()
        s = line[0]
        t = line[1]
        labels = line[2:]
        #print(labels)
        if query_num == 100:
            break;
            #print(s,t,labels)
        query = "MATCH (a)-[p:`"+str(labels[0])+"`";
        for w in range(1,len(labels)):
            query += "|:`"+labels[w]+"`"
        query += "*.."+str(k)+"]-(b) WHERE a.id="+str(s)+" AND b.id="+str(t)+" RETURN a LIMIT 1"
        print(query)
        rel = test_graph.run(cypher=query).data()
        if not rel:
            print("ERROE",s,t)
        time_e2=time.time()
        f_true_out.write(str(time_e2-time_s2)+"\n")
        #print(labels)
        #print(query)
    f_true.close()
#false queries
    for i in f_false.readlines():
        query_num +=1
        if query_num == 200:
            break;
        line = i.split()
        s = line[0]
        t = line[1]
        labels = line[2:]
        time_s2=time.time()
        query = "MATCH (a)-[p:`"+str(labels[0])+"`";
        for w in range(1,len(labels)):
            query += "|:`"+labels[w]+"`"
        query += "*.."+str(k)+"]-(b) WHERE a.id="+str(s)+" AND b.id="+str(t)+" RETURN a"
        rel = test_graph.run(cypher=query).data()
        if rel:
            print("Error")
        time_e2=time.time()
        f_false_out.write(str(time_e2-time_s2)+"\n")
       # else:
        #    print("False")
        #print(labels)
        #print(query)
    f_false.close()
    f_true_out.close()
    f_false_out.close()
    #k+=2
#rel = test_graph.run(cypher="MATCH (a) where a.id='2' RETURN a").data()
#"MATCH (a)-[p:`1271`|:`30742`|:`36307`|:`50767`|:`53045`*..3]->(b) WHERE a.id=13792449 AND b.id=14750757 RETURN a"
#print(rel)

time_end=time.time()
print('time cost',time_end-time_start,'s')

