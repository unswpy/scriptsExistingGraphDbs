#from py2neo import Graph,Node,Relationship
import time,sys
from SPARQLWrapper import SPARQLWrapper ,JSON

def query_state(k, labels, s, t):
    prefix = " <file:///projects/NNSGroup/youpeng/VLDBJ_LCKR_revision/apache-jena-fuseki-4.0.0/"
    rdf_s = prefix+str(s)+"> "
    rdf_t = prefix+str(t)+"> "
    label_set = "( "
    index = 0
    query_string = "SELECT '" +rdf_s + "' { "
    for label in labels:
        if index == 0:
            label_set += prefix + str(label) + "> "
        else:
            label_set+= " | "+prefix + str(label) +"> "
        index += 1
    label_set += ") "
    for i in range(0,k):
        query_k = "{ "
        if i == 0:
            query_k += rdf_s + label_set + rdf_t +" . "  
        else:
            query_k += rdf_s + label_set + "$o"+str(1) +" . "  
            for j in range(1,i+1):
                #query_k += rdf_s + label_set + "$o"+str(j+1) +" . "
                if j == i+2:
                    query_k += "$o" + str(j-1) + label_set + rdf_t +" . "
                elif j < i:
                    query_k += "$o" + str(j) + label_set + "$o"+str(j+1) +" . "
                else:
                    query_k += "$o" + str(j) + label_set + rdf_t + " . "
        query_string += query_k
        query_string += "}"
        if i < k-1:
            query_string += "UNION \n " 
    query_string += " } LIMIT 1"
    return query_string
        #query_k += ""
        #print(i)
        #label_set += str(label)





if len(sys.argv) != 2:
    print("wrong argv")
    exit(33)
#query_name = sys.argv[1]
k=int(sys.argv[1])
print("k="+str(k))
time_start=time.time()
query_name = "dbpediaL53046.edge"
if True:
    #filename = query_name+str(k)+".true"
    
    f_true_out = open(query_name+str(k)+".true_runtime", "w")
    f_false_out = open(query_name+str(k)+".false_runtime","w")
    
    f_true = open(query_name+str(k)+".true", "r")
    f_false = open(query_name+str(k)+".false","r")
    query_num = 0
    sparql_website="http://localhost:3030/dbpedia/query"
    for i in f_true.readlines():
        time_s2=time.time()
        query_num +=1
        print(query_num)
        line = i.split()
        s = line[0]
        t = line[1]
        labels = line[2:]
        #print(labels)
        query = ""
        #query = "PREFIX : <http://localhost:3030/dbpedia/query>"
        query = query_state(k,labels,s,t)
        if query_num == 100:
            break;
            #print(s,t,labels)
        #query = "MATCH (a)-[p:`"+str(labels[0])+"`";
        #for w in range(1,len(labels)):
        #    query += "|:`"+labels[w]+"`"
        #query += "*.."+str(k)+"]-(b) WHERE a.id="+str(s)+" AND b.id="+str(t)+" RETURN a LIMIT 1"
        #print(query)
        #rel = test_graph.run(cypher=query).data()
        sparql = SPARQLWrapper(sparql_website)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        #print(results["results"]["bindings"])
        if not results["results"]["bindings"]:
            print("true ERROE",s,t)
        time_e2=time.time()
        f_true_out.write(str(time_e2-time_s2)+"\n")
        #print(labels)
        #print(query)
    f_true.close()
#false queries
    for i in f_false.readlines():
        #break;
        query_num +=1
        if query_num == 200:
            break;
        line = i.split()
        s = line[0]
        t = line[1]
        labels = line[2:]
        query = ""
        #query = "PREFIX : <http://localhost:3030/>"
        query += query_state(k,labels,s,t)
        time_s2=time.time()
        #print(query)
        #query = "MATCH (a)-[p:`"+str(labels[0])+"`";
        #for w in range(1,len(labels)):
        #    query += "|:`"+labels[w]+"`"
        #query += "*.."+str(k)+"]-(b) WHERE a.id="+str(s)+" AND b.id="+str(t)+" RETURN a"
        sparql = SPARQLWrapper(sparql_website)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        #print(results["results"]["bindings"])
        if results["results"]["bindings"]:
            print("false Error")
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

