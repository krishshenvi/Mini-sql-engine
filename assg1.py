import sqlparse
import itertools
import sys
import csv


def reading_meta(lines):
        
    jj={}
    pp=0
    kk=lines
    for i in kk:
        i=i.rstrip('\r\n')
        if(pp==0):
            table_name=i
            table_name = table_name.lower()
            pp=1
            jj[table_name]=[]
            continue   
        elif('<end_table>' in i):
            pp=0     
        elif(pp):
            jj[table_name].append(i.lower())
    return jj



def eliminate_db(database):
    for i in database:
        for j in range(len(database[i])):
            database[i][j]=database[i][j].rstrip('\r\n')
           # print(database[i][j])
            database[i][j]=database[i][j].split(',')
            #print(database[i][j])
            for k in range(len(database[i][j])):
                database[i][j][k]=int(database[i][j][k])
                #print(database[i][j][k])
   
    return database

def checkif_distinct(query):

    if(query[1] == "distinct"):
        distinct = 1
    else:
        distinct = 0
    return distinct

def checkif_incorrect(query,distinct, db_column, order_by):
    flag=True

    if(query[0]!='select'):
        
        flag=False

    #print(query[4+distinct][0:9])

    if len(query) > 10:
        flag = False
    if len(query) < 4:   
        flag = False

    
    elif(query[2+distinct]!='from'):
        flag=False
        
    
    
    elif(4+distinct<len(query)) and (query[4+distinct][0:6]!='where '):
        if 4+distinct<len(query) and (query[4+distinct][0:9]!='group by'):
            if 4+distinct<len(query) and (query[4+distinct][0:9]!='order by'):
                flag=False
            #print("hi")

    #setting up the flag finally


    if flag == False:
        print("Wrong syntax")
        exit(0)


def retrieve_tables(query, distinct, db_column, order_by):

    final_table_list = []
    try:
        index = 3 + distinct
        tables_list=query[index].split(",")
        for i in range(len(tables_list)):
            tables_list[i]=tables_list[i].strip()
        final_table_list.append(tables_list)
                
    except:
        print("Error: From clause not parsed")
        exit(0)

    db_table_list = []
    for tables in db_column:
        db_table_list.append(tables)

    for i in tables_list:
        if i in db_table_list:
            continue
        else:
            print("Oops, the table mentioned is not present")
            exit(0)
        
    return tables_list



def jointable(tables,db_column):
    """
    Joins all tables which are mentioned in from
    """

    if len(tables) == 0:
        print("Oops, there are no tables present in your query.")
        exit(0)

    fields=[]#stores col name like tablename.attribute
    fields_just_the_column_name=[]#stores col name like attribute
    aux2=[]#stores list after converting aux to a list
    temp = tables[0]
    final_tables = []
    aux=db_column[temp]#cumulative store of joins
    
    res = str()
    res = res+ "table1"
    res = str(list(aux.keys())[0])

    #find no of records in aux and append column header
    noraux=0
    for k in aux:
        fields.append(str(tables[0])+'.'+str(k))
        final_tables.append(str(k))
        fields_just_the_column_name.append(str(k))
        
    noraux=len(aux[res])
    auxillary = []
    #covert aux to a list
    for l in range(noraux):
        row3=[]
        for m in aux.keys():
            auxillary.append(aux[m][l])
            row3.append(aux[m][l])
        aux2.append(row3)

    #copy aux2 back to aux
    aux=aux2.copy()

    aux2.append(1)
    aux2.clear()

    #print(aux2)
    final_fields = []
    #return if only 1 table
    if(len(tables)==1):
        return [aux,fields,fields_just_the_column_name]

    #for multiple tables
    for i in range(1,len(tables)):
        aux2=[]#empty aux2 after every iteration
        a=db_column[tables[i]]#next table

        #find no of records in a
        for k in a:
            fields.append(str(tables[i])+'.'+str(k))
            final_fields.append(str(k))
            fields_just_the_column_name.append(str(k))
            
    
        res = str(list(a.keys())[0])
        nora = len(a[res])

        #perform join on aux and a
        for l in aux:
            for k in range(nora):
                row=[]
                for j in a.keys():
                    row.append(a[j][k])
                aux2.append(l+row)

        #copy aux2 back to aux
        aux=aux2

    return [aux,fields,fields_just_the_column_name]



def getcond(query,distinct):
    """
    Get all the conditions specified in where clause
    """

    global where_flag
    where_flag = False
    if len(query) > distinct+4 and query[distinct+4][0:6] == "where ":
       

        where_flag = True

        possible_operators = [" and ", " or ", " AND ", " OR " ]
        finalops = []
        given_cond = query[4+distinct]
        given_cond = given_cond[6:].strip()

       # print(given_cond)

        for ops in possible_operators:
            if ops not in given_cond:
                continue
            else:
                given_cond = given_cond.replace(ops, ",")
                given_cond = given_cond.split(",")
                finalops.append(given_cond[0])
                ops = ops.split()
                return [given_cond[0:2], ops]
        return [given_cond.split(","), []]
    else:
        return [[], []]


def checkcond(cond,fh,fh2,tables, order_by):

    """
    Check if conditions specified in where clause are correct or not
    """
    flag = False
    ops_inquery = []
    check_ops = []
    possible_operators = [">=", "<=", "=", ">" , "<"]
    check_ops.append(possible_operators[0])

    for checks in cond:
        for ops in possible_operators:
            if ops in checks:
                flag = True
                ops_inquery.append(ops)
                check_ops.append(ops)
                break
        if flag != True:
            print("Error in conditions present in where clause")
            exit(0)

    #print(cond)
    
    for i in range(len(cond)):
        cond[i] = cond[i].replace(ops_inquery[i], ",")
        temp = cond[i].split(",")


        for i in range(len(temp)):
            try:
                temp[i] = int(temp[i])
            except:
                pass

        #print(temp[i])

        for variables in temp:
            if(type(variables) == str):
                variables = variables.strip()
                #variables = variables.split()

                #print(variables)

                if variables in fh2:
                    continue
                else:
                    print("Error: Attributes not specified properly in where clause")
                    exit(0)

    return ops_inquery

def returnindex(cond,fh,fh2):
    """
    Returns index of attributes used in cond
    """
    cond=cond.split(",")
    #print(cond)
    final_index = []

    for i in range(len(cond)):
        try:
            cond[i]=int(cond[i])
        except:
            continue
    
    if type(cond[0]) == str:
        cond[0] = cond[0].strip()
        if cond[0] not in fh:
            for atts in fh:
                atts = atts.split(".")
                final_index.append(atts[0])
                atts = atts[0]
                atts = str(atts) + "." + str(cond[0])
                final_index.append(atts)
                if atts in fh:
                    for i in range(len(fh)):
                        if atts == fh[i]:
                            att1 = i
                            #print(att1)
                            break
                    else:
                        continue

                    break
    else:
        att1 = str(cond[0])
    
    final_index = []

    if type(cond[1]) == str:
        cond[1] = cond[1].strip()
        if cond[1] not in fh:
            #print("test")
            for atts in fh:
                atts = atts.split(".")
                final_index.append(atts[0])
                atts = atts[0]
                atts = str(atts) + "." + str(cond[1])
                final_index.append(atts)
                #print("test")
                if atts in fh:
                    #print("test")
                    for i in range(len(fh)):
                        if atts == fh[i]:
                            att2 = i
                            break
                    else:
                        continue

                    break
    else:
        att2 = str(cond[1])

    return[att1,att2]



def checkexpr(a1,a2,i,opused):
    """
    Generates expression according to condition
    """

    #both are attributes
    if type(a1) == int and type(a2) == int:
        temp = str(i[a1])+str(opused)+str(i[a2])
        return temp

    #a2 is not an attribute
    elif type(a1) == int and type(a2) == str:
        temp = str(i[a1])+str(opused)+a2
        return temp
    
    #a1 is not an attribute
    elif type(a1) == str and type(a2) == int:
        temp = a1+str(opused)+str(i[a2])
        return temp
    
    #both are not attributes
    else:
        temp = a1+str(opused)+a2
        return temp

def applycond(jt,fh,fh2,cond,opused,andor):
    """
    remove records form joined table if they do not meet conditions
    """
    ans_without_where = []
    if len(cond) == 0:
        for i in jt:
            ans_without_where.append(i)

        return ans_without_where
    

    cond_applied = []
    for units in opused:
        if units == "=":
            ind = opused.index("=")
            opused[ind] = "=="
    #print(cond[0])
    conditions_final = []
    #if no and/or in where clause
    if(len(andor)==0 and len(cond) == 1):
        conditions_final.append(cond)
        a1,a2=returnindex(cond[0],fh,fh2)
        ans=[]
        for i in jt:
            cond_applied.append(i)
            temp=checkexpr(a1,a2,i,opused[0])
            if not eval(temp):
                continue
            else:
                ans.append(i)#if cond sattisfied then append
        return ans

    #if and/or in where clause
    elif(len(andor)==1):

        #process and/or
        #print(andor)
        andor = andor[0].lower()
        andor = " " + andor +  " "


        a1,a2=returnindex(cond[0],fh,fh2)
        b1,b2=returnindex(cond[1],fh,fh2)

        ans=[]
        ans.clear()
        for i in jt:
            temp=checkexpr(a1,a2,i,opused[0])
            temp2=checkexpr(b1,b2,i,opused[1])
            temp3=str(temp)
            cond_applied.append(temp3)
            temp3 = temp3 + str(andor)
            temp3 = temp3 + str(temp2)
            if eval(temp3) == True:
                ans.append(i)
        return ans

    else:
        print("Error: Too many conditions")
        exit(-1)


def selectpreprocess(s):
    """
    Remove leading and trailing spaces from attributes
    """

    s=s.split(',')
    for i in range(len(s)):
        s[i]=s[i].strip()
    temp = str()
    for i in range(len(s)):
        if i == 0:
            temp = temp + s[i]
        else:
            temp = temp + "," + s[i]
    #print(temp)
 
    return temp

def checkagg(att,temp,distinct):
    """
    Check if aggregate functions used or not
    """

    #print(temp)
    
    agg=[]
    aggarray=["max()","min()","sum()","avg()", "count()"]
    if(len(att)>5):
        if "(" in att:
            i = att.index("(") + 1
            a = att[0:i] + ")"
            #print(a)
            if a in aggarray:
           
                att = att[i:-1]
                #print(att)
                
                temp_var = att
                #check for distinct in att name
                att=att.strip()
                

                agg.append(a)

    #print(agg)
    #print(att)

    return [att,agg,distinct]



def checkselect(query,fh,fh2,distinct):
    """
    Check if select statement is correct or not
    """

    global cols_with_agg, agg_funcs

    cols_with_agg = []
    agg_funcs = []


    #if select *
    if(query[1+distinct]=="*"):
        return [["*"],[],distinct,[], ["*"]]

    else:
        temp=query[1+distinct].split(",")
        #print(temp)
        for i in range(len(temp)):
            
            att=temp[i]
            att,agg,distinct=checkagg(att,temp,distinct)
            #print(agg)
            if distinct == 1:
                var = distinct
            if len(agg) == 0:
                temp_var = agg
                temp_var.append(att)
            else:
                cols_with_agg.append(att)
                agg_funcs.append(agg[0])
            temp[i]=att

            #print(fh2.count(att))
            #print(att)

            if (fh2.count(att)!=1 and att != "*"):
                print("Error: Attributes not specified properly in select clause")
                exit(-1)

        #print(temp)
        cols_select = []
        for i in range(len(temp)):
            temp[i]=temp[i].strip()
            if temp[i] not in fh:
                for atts in fh:
                    atts = atts.split(".")
                    atts = atts[0]
                    atts = str(atts) + "." + str(temp[i])
                    cols_select.append(atts)
                    if(atts in fh):
                        temp[i] = atts
            else:
                temp[i]= temp[i]

        #print(temp_var)

        if len(agg) == 0:
            agg = temp_var
        
        if(len(cols_with_agg) != 0):
            for i in range(len(cols_with_agg)):
                if cols_with_agg[i] not in fh:
                    for atts in fh:
                        atts = atts.split(".")
                        atts = atts[0]
                        atts = str(atts) + "." + str(cols_with_agg[i])
                        if(atts in fh):
                            cols_with_agg[i] = atts
        #print()



        
        #print(cols_with_agg)
        sel_columns = []
        sel_columns = temp.copy()
        

        dict_map_aggfunc = {}

        for i, j in zip(cols_with_agg, agg_funcs):
            dict_map_aggfunc[i] = j
        #print(dict_map_aggfunc)
        #sel_columns = temp

        for i in range(len(temp)):
            if temp[i] in dict_map_aggfunc.keys():
                str1 = dict_map_aggfunc[temp[i]]
                temp[i] = str1[0:-1] + temp[i] + ")"
        
        #print(sel_columns)
            

        return [temp,cols_with_agg,distinct, agg_funcs, sel_columns]

def removeduplicate(ans):
    """
    Removes dupliacte columns before printing
    """

    if(len(ans)==0):
        return [[],[]]
    else:
        #transpose
        ans=list(zip(*ans))

        #get unique
        tables = []
        uniqans=[]
        index=[]
        for i in range(len(ans)):
            if ans[i] not in uniqans:
                tables.append(ans[i])
                uniqans.append(ans[i])
            else:
                index.append(i)

        #transpose back
        uniqans=list(zip(*uniqans))
        tables=list(zip(*tables))

        #convert to list
        for i in range(len(uniqans)):
            tables[i] = list(tables[i])
            uniqans[i]=list(uniqans[i])

        return [uniqans,index]



def avg(x):
    """
    Return avg of list elements
    """
    
    temp = sum(x) / len(x)
    temp = round(temp, 3)
    return temp



def niceprint(x,agg):
   
    cellwidth = 15
    table_width = 5
    #print each record
    if agg == 0:
        print('< ',end='')
        for i in range(len(x)):
            x[i]=str(x[i])
            table_width = table_width + 5
            #i=i.center(cellwidth)#ljust,rjust,center
            if i == len(x)-1:
                print(x[i], end = " ")
            else:
                print(x[i], end = ", ")
        print(' > ',end='')
    else:
        print('',end='')
        for i in range(len(x)):
            x[i]=str(x[i])
            table_width = table_width + 5
            #i=i.center(cellwidth)#ljust,rjust,center
            if i == len(x)-1:
                print(x[i],end=' ')
            else:
                print(x[i],end=',')
        table_width = table_width + cellwidth

    #print newline after each record
    print()



def printagg(agg,arr):
    """
    Apply aggregate functin and then print
    """

    #print(arr)

    print_var = []
    
    #print(length)
    if type(arr) == int:
        niceprint([arr], 1)
    else:
        
        #print(exp)
        list_evals = []
        if(len(arr) == 0):
            niceprint([0], 1)
            exit(0)
        length = len(arr[0])
        if "count()" in agg:
            niceprint([len(arr)], 1)
            #print("hi")
        else:
            #print("hi")
            eval_arr = []
            for i in range(0,length):
                list_agg = []
                for j in range(len(arr)):
                    print_var.append(arr[j][i])
                    list_agg.append(arr[j][i])
                #print(agg)
                exp=agg[i][0:-1]
                list_evals.append(exp)
                exp+=str(list_agg)+")"
                #print(eval(exp))
                

                eval_arr.append(eval(exp))


            #print(eval_arr)
            list_evals.append(eval_arr)
            niceprint(eval_arr,1)


def findindex(att,fh):
    """
    Find index of att in fh
    """

    for i in range(len(fh)):
        if(att==fh[i]):
            return i



def printresult(fh,ans,distinct,cols,agg_funcs, agg, sel_columns, group_by):
    """
    Print query result
    """
    #print(sel_columns)
    if sel_columns == ["*"] and group_by == True:
        print("error in query as * cannot be there with group by")
        exit(-1)
    

    elif sel_columns == ["*"] and "count()" in agg_funcs:
        niceprint(cols, 0)
        printagg(agg_funcs, len(ans))
    elif(sel_columns==["*"]):
        
        #remove duplicate columns
        
        ans,index=removeduplicate(ans)

        #remove col header of duplicate cols
        for i in index:
            fh.pop(i)

        #print col names
        niceprint(fh,0)
        table_var = []
        #check if distinct present
        if(distinct==1):
            uniqans=[]
            for i in ans:
                if i in uniqans:
                    continue
                else:
                    table_var.append(i)
                    uniqans.append(i)
            for i in uniqans:
                niceprint(i,1)
        else:
            for i in ans:
                table_var.append(i)
                niceprint(i,1)

    else:
        #get index of attributes present in query
        
        index=[]
        #print(agg)
        #print(sel_columns)
        for att in sel_columns:
            index.append(findindex(att,fh))
        
        #print col names
        #print(index)
    
        
        niceprint(cols, 0)
        
        table_var = []
        #extract cols from joined table
        ans2=[]
        for record in ans:
            tr=[]
            for i in index:
                table_var.append(record[i])
                tr.append(record[i])
            ans2.append(tr)
        #print(ans2)

        if(distinct==1):
            uniqans2=[]
            for i in ans2:
                if i in uniqans2:
                    continue
                else:
                    table_var.append(i)
                    uniqans2.append(i)
            if(len(agg_funcs)==0):
                for i in uniqans2:
                    niceprint(i,1)
            else:
                printagg(agg,uniqans2)
        else:
            if(len(agg_funcs)!=0):
                printagg(agg_funcs,ans2)
                
            else:
                for i in ans2:
                    table_var.append(i)
                    niceprint(i,1)
                #print(ans2)
               

def check_groupby(query, fh, fh2, distinct, where_flag):

    global groupby_col

    if where_flag == False and len(query) > 5+ distinct and query[4+distinct][0:9] == "group by":
        groupby_col = query[5+distinct]
        #print(groupby_col)
        return True

    elif where_flag == True and len(query) > 5+distinct and query[5 + distinct] == "group by":
        groupby_col = query[6+distinct]
        return True
    else:
        return False

def apply_aggregate(final_dict, map_index_with_agg):

    #print(map_index_with_agg)
    #print(final_dict)
    for keys in final_dict.keys():
        if keys in map_index_with_agg.keys() and map_index_with_agg[keys] == "avg()":
            final_ans = avg(final_dict[keys])
            final_dict[keys].clear()
            final_dict[keys] = final_ans
            #print("hi")
        
        elif keys in map_index_with_agg.keys() and map_index_with_agg[keys] == "min()":
            exp = "min(" + str(final_dict[keys]) + ")"
            final_ans = eval(exp)
            final_dict[keys].clear()
            final_dict[keys] = final_ans
        
        elif keys in map_index_with_agg.keys() and map_index_with_agg[keys] == "max()":
            exp = "max(" + str(final_dict[keys]) + ")"
            final_ans = eval(exp)
            final_dict[keys].clear()
            final_dict[keys] = final_ans
        
        elif keys in map_index_with_agg.keys() and map_index_with_agg[keys] == "sum()":
            exp = "sum(" + str(final_dict[keys]) + ")"
            final_ans = eval(exp)
            final_dict[keys].clear()
            final_dict[keys] = final_ans

        elif keys in map_index_with_agg.keys() and map_index_with_agg[keys] == "count()":
            
            final_ans = len(final_dict[keys])
            final_dict[keys].clear()
            final_dict[keys] = final_ans
        
    return final_dict      
            


def apply_groupby(ans, query, fh, fh2, distinct, agg, cols):
    
    #print(cols)
    #print(agg_funcs)
    #print(cols_with_agg)
    #print(fh2)

    map_index_with_agg = {}

    cols_index = []

    for columns_agg in cols_with_agg:
        cols_index.append(findindex(columns_agg, fh))
    
    for i, j in zip(cols_index, agg_funcs):
        map_index_with_agg[i] = j
    
    #print(map_index_with_agg)

    #print(agg)
    if len(agg_funcs) == 0:
        indexes = []
        for vals in range(len(cols)):
            #print(vals)
            str1 = cols[vals][7:]
            #print(str1)
            i = findindex(str1, fh2)
            indexes.append(i)
            #print(i)
            if i in map_index_with_agg.keys():
                temp = map_index_with_agg[i]
                cols[vals] = temp[0:-1] + cols[vals] + ")"
        for outer_lists in ans:
            for i in range(len(outer_lists)):
                if i not in indexes:
                    print(i)
                    try:
                        outer_lists.remove(outer_lists[i])
                    except:
                        print("Error in attrbutes present in Select clause")
                        exit(-1)

                
        return [ans, 1, cols]
    else:
        #print(groupby_col)
        dict1 = {}
        #print(groupby_col)
        #print(fh2)
        ind = findindex(groupby_col, fh2)
        for outer_lists in ans:
            temp = []
            #print(ind)
            for i in range(0, len(outer_lists)):
                if i == ind:
                    #print(i)
                    if outer_lists[i] not in dict1.keys():
                        dict1[outer_lists[i]] = []
                    
                    var = outer_lists[i]
                    #print(var)
                    temp.append(outer_lists[i])
                else:
                    temp.append(outer_lists[i])
                    #print(temp)
                
                #print(temp)
                
            dict1[var].append(temp)

        
        #print(cols)
        #print(dict1)

        final_ans = []
        for dict_values in dict1.values():
            sub_ans = []
            final_dict = {}     
            for i in range(len(fh2)):
                if i != ind:
                    final_dict[i] = []
                sub_ans.append(0)
            for lists in dict_values:
                for i in range(len(lists)):
                    if i == ind:
                        var = lists[i]
                        continue
                    else:
                        final_dict[i].append(lists[i])
            
            #print(final_dict)
            
            ans_dict = apply_aggregate(final_dict, map_index_with_agg)
            #print(ans_dict)
            for i in range(len(fh2)):
                if i != ind:
                    sub_ans[i] = ans_dict[i]
                else:
                    sub_ans[i] = var
            final_ans.append(sub_ans)

        #print(map_index_with_agg)
        #print(fh)

        #print(cols)

        indexes = []
        for vals in range(len(cols)):
            #print(vals)
            str1 = cols[vals][7:]
            #print(str1)
            i = findindex(str1, fh2)
            indexes.append(i)
            #print(i)
            if i in map_index_with_agg.keys():
                temp = map_index_with_agg[i]
                cols[vals] = temp[0:-1] + cols[vals] + ")"
                #print("hi")
        
        #print(cols)
        #print(final_ans)
        #print(indexes)
        '''
        for j in range(len(final_ans)):
            for i in range(len(final_ans[j])):
                if i not in indexes:
                    #print(i)
                    final_ans[j].remove(final_ans[j][i])
        '''         
        return [final_ans, 0, cols]



        


        
        
        #print(dict1)


        
def check_agg(col_order_by):
    flag = False
    var = col_order_by
    if "(" in col_order_by:
        flag = True
        if col_order_by.index("(") == 3:
            var = col_order_by[4:-1]
        elif col_order_by.index("(") == 5:
            var = col_order_by[6:-1]
    return [var, flag]



def processquery(query,db_column):
    
    global order_by
    order_by = False
    #check if distinct present
    distinct=checkif_distinct(query)

    #check if incorrect
    checkif_incorrect(query,distinct, db_column, order_by)

    #process from clause
    tables=retrieve_tables(query,distinct, db_column, order_by)

    index = []

    jt,fh,fh2 = jointable(tables,db_column)
    
    cond,andor=getcond(query,distinct)
    #print(index)
    #print(andor)
    ans = []
    opused = []
    opused=checkcond(cond,fh,fh2,tables, order_by)
    ans=applycond(jt,fh,fh2,cond,opused,andor)

    #process select clause
    groupby = check_groupby(query, fh, fh2, distinct, where_flag)

    
    query[1+distinct]=selectpreprocess(query[1+distinct])
    cols,agg,distinct, agg_funcs, sel_columns=checkselect(query,fh,fh2,distinct)

    #print(agg)
    #print(cols)
    #print(sel_columns)
    
    for att in sel_columns:
            index.append(findindex(att,fh))

    #print
    #print(groupby)
    if(groupby):
        ans, distinct, fh = apply_groupby(ans, query, fh, fh2, distinct, agg, sel_columns)

        if "order by" in query:
            #print("hi")
            get_index = query.index("order by")
            col_order_by = query[get_index + 1]
            col_order_by = col_order_by.split()
            exact_col, flag = check_agg(col_order_by[0])
            col_order_by[0] = exact_col

            if flag == True and ("asc" in query or "desc" in query):
                sort_order = query[get_index + 2]

            elif len(col_order_by) == 2:
                sort_order = col_order_by[1]
            else:
                sort_order = "asc"

            ind = findindex(col_order_by[0], fh2)


            if sort_order == "asc":
                #print("hi")
                ans = sorted(ans, key=lambda x: x[ind]) 
            else:
                ans = sorted(ans, key=lambda x: x[ind], reverse = True)
        niceprint(cols, 0)
        
        #print(sel_columns)
         #check if distinct present
        final_execution = []
        if(distinct==1):
            uniqans=[]
            for i in ans:
                if i not in uniqans:
                    final_execution.append(i)
                    uniqans.append(i)
            for i in uniqans:
                final_execution.append(i)
                niceprint(i,1)
        else:
            ans2=[]
            #print(ans)
            for record in ans:
                tr=[]
                for i in index:
                    #print(i)
                    final_execution.append(record[i])
                    tr.append(record[i])
                ans2.append(tr)
            for i in ans2:
                niceprint(i, 1)
    else:
        #print("hi")
        if len(agg_funcs) != 0 and len(agg_funcs) != len(sel_columns):
            print("INVALID QUERY. without group by all attributes must contain agg functions.")
            exit(-1)
        if "order by" in query:
            #print("hi")
            get_index = query.index("order by")
            col_order_by = query[get_index + 1]
            col_order_by = col_order_by.split()
            exact_col, flag = check_agg(col_order_by[0])
            col_order_by[0] = exact_col
            if len(col_order_by) == 2:
                sort_order = col_order_by[1]
            else:
                sort_order = "asc"

            ind = findindex(col_order_by[0], fh2)


            if sort_order == "asc":
                #print("hi")
                ans = sorted(ans, key=lambda x: x[ind]) 
            else:
                ans = sorted(ans, key=lambda x: x[ind], reverse = True)
        printresult(fh,ans,distinct,cols,agg_funcs, agg, sel_columns, groupby)



if __name__ == '__main__':
        f=open('metadata.txt','r')
        lines=f.readlines()
        for i in lines:
            if('<begin_table>' in i):
                lines.remove(i)
        metadata=reading_meta(lines)

        #print(metadata)


        dict_column = {}
        for i in metadata:
            dict_column[i] = len(metadata[i])
        
        #print(dict_column)

        #print(metadata)
        database={}
        for i in metadata:
            ff=open(i+'.csv','r')
            data=ff.readlines()
            database[i]=data
        #print(database)
        database_rowwise = eliminate_db(database)
        #print(database)
        #print(metadata)
        database_columnwise = {}
        for i in metadata:
            database_columnwise[i] = {}
            temp = dict_column[i]
            #print(temp)
            for j in range(temp):
                with open(i+'.csv', 'r') as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=',')
                    #print(metadata[i][j])
                    database_columnwise[i][metadata[i][j]] = []
                    for lines in csv_reader:
                        database_columnwise[i][metadata[i][j]].append(int(lines[j]))
                #print("...........................")
        #print(database_columnwise)
        

        var = sys.argv[1]
        var = var.lower()
        if(var[-1:] != ";"):
            print("Check karle bhai syntax")
            exit(0)
        
        var = var[:-1]
        sqlparse.format(var, keyword_case='upper')

        parsed = sqlparse.parse(var)
        command = parsed[0]

        token_list = sqlparse.sql.IdentifierList(command.tokens).get_identifiers()

        query_list = []

        for i in token_list:
            query_list.append(str(i))
        
        #print(query_list)

        processquery(query_list, database_columnwise)
        