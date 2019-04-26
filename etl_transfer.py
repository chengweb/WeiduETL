# -*- coding:utf-8 -*-

import pymysql
import functools
import json
import time
import logging


def logger_(name):
    log = logging.getLogger()

    # 设置logger
    fh = logging.FileHandler(name)  # 向文件发送信息
    ch = logging.StreamHandler()  # 向屏幕发送信息

    # 定义格式
    fm = logging.Formatter('%(levelname)s %(asctime)s %(filename)s %(lineno)d %(message)s')
    fh.setFormatter(fm)  # 设置fh的格式
    ch.setFormatter(fm)  # 设置ch的格式

    if log.handlers:  # 清空logger.handlers，否则会重复打印日志
        log.handlers.pop()

    log.addHandler(fh)  # 传给logger
    log.addHandler(ch)  # 传给logger
    log.setLevel('DEBUG')

    return log


logger = logger_("sql_transfer.log")


def try_catch2(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):

        return func(*args, **kwargs)
    return inner


def try_catch(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        name = kwargs.get("name")
        try:
            logger.info(u"%s开始" % name)
            ret = func(*args, **kwargs)
            logger.info(u"%s结束" % name)
            return ret
        except Exception, e:
            logger.error("执行%s出错" % name)
            json.dump(args[0], open("%s_args.json" % func.__name__, 'w'))
            raise e

    return inner


class MySqlConn:
    def __init__(self, host, port, db, user, pwd):
        self.conn = pymysql.connect(host=host, port=port, database=db, user=user, password=pwd)
        self.cursor = self.conn.cursor()

    def get_data(self, sql, *args):
        rows = self.cursor.execute(sql, args)
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()


# 人员列表
@try_catch
def list_people(conn, pro_id, sur_id, name):

    sql = """
        SELECT
        people_id
        FROM front_peoplesurveyrelation
        where
        is_active=true 
        and project_id=%s
        and survey_id=%s
        order by people_id
        limit 100
        """
    res = conn.get_data(sql, pro_id, sur_id)
    return res


# 问卷答案
@try_catch
def get_answer(conn, pro_id, sur_id, name):
    sql = """
            SELECT
            people_id, question_id, sum(answer_score) as answer_score
            FROM front_userquestionanswerinfo a,
            (select max(id) id FROM front_userquestionanswerinfo 
            where project_id=%s and survey_id=%s and is_active=true
            group by people_id,question_id,answer_id
            ) b
            where a.id=b.id
            group by people_id,question_id
            order by people_id,question_id
            limit 100;
        """
    res = conn.get_data(sql, pro_id, sur_id)
    # ((people_id, question_id, sum(answer_score)), (people_id, question_id, sum(answer_score)), ...)
    return res


# 题目编号
@try_catch
def question_tag(conn, t_id, name):
    sql = """
        SELECT
        object_id
        , tag_value
        FROM research_questiontagrelation
        where is_active=true 
        and tag_id=%s
        order by object_id
        limit 343,100;
        """
    res = conn.get_data(sql, t_id)
    return res  # ((object_id, tag_value), )


@try_catch
def merge_left(left, right, lindex, rindex, name):
    res = []
    for i in left:
        i = list(i)
        for j in right:
            import copy
            ret = copy.deepcopy(i)
            if ret[lindex] == j[rindex]:
                j = list(j)
                ret.extend(j)
            else:
                ret.extend([None]*len(j))
            res.append(ret)
    return res


#################
#################
@try_catch
def select_values(tpls, name):
    res = []
    for tpl in tpls:
        lst = [tpl[0], tpl[2], tpl[-1]]
        if lst not in res:
            res.append([tpl[0], tpl[2], tpl[-1]])
    return res


# Row denormaliser
@try_catch
def row_denormaliser(value_lists, name):
    res = {}
    for lst in value_lists:
        key = lst[0]
        if key not in res:
            res[key] = {"score": [], "tag_value": []}
        else:
            res[key]["score"].append(lst[1])
            res[key]["tag_value"].append(lst[2])
    ret = []
    for pid in res:
        line = [pid]
        line.extend(res[pid]["score"])
        line.extend(res[pid]["tag_value"])
        ret.append(line)
    return ret


# ***************人员列表、问卷答案************
def line3(conn, pro_id, sur_id, t_id, *args):
    admin_conn = args[0]
    # 人员列表 ((people_id, ), (people_id, ), ...)
    people_id_tuples = list_people(conn, pro_id, sur_id, name=u"数据库查询人员列表")
    # 问卷答案 ((people_id, question_id, answer_score), ...)
    # answer_tuples = get_answer(conn, pro_id, sur_id, name=u"数据库查询问卷答案")
    # json.dump(answer_tuples, open("answer_tuples.json", "w"))

    answer_tuples = json.load(open("answer_tuples.json"))

    merge_join_2 = merge_left(answer_tuples, people_id_tuples, 0, 0, name=u"合并问卷答案和人员列表")
    # Sort rows 4 3
    merge_join_2.sort(key=lambda x: x[1])
    # 题目编号  (object_id, tag_value)
    question_tag_tuples = question_tag(admin_conn, t_id, name=u"数据库查询题目编号")
    # Merge Join [[people_id, question_id, answer_score, people_id, object_id, tag_value], ...]

    # merge_join = merge_left(merge_join_2, question_tag_tuples, 1, 0, name=u"合并问卷答案、人员列表和题目编号")
    merge_join = merge_inner(merge_join_2, question_tag_tuples, 1, 0, name=u"合并问卷答案、人员列表和题目编号")
    # Select values

    select_value = select_values(merge_join, name=u"筛选people_id, answer_score, tag_value")

    # Sort rows 4 4 [[people_id, answer_score, tag_value], ...]
    select_value.sort(key=lambda x: (x[0], x[2]))
    # Row denormaliser [[people_id, answer_score1, answer_score2, ..., tag_value1, tag_value2, ...], ...]
    # RowDenormaliser = row_denormaliser(select_value[:1000], name=u"列转行")
    RowDenormaliser = row_denormaliser(select_value, name=u"列转行")
    # Sort rows 4
    RowDenormaliser.sort(key=lambda x: x[0])
    sort_rows_4 = RowDenormaliser
    return sort_rows_4


# ***************人员基础信息************
@try_catch
def people_base_info(conn, parentid, wduser_pid, name):
    sql = """
        SELECT
          a.id
        , a.username as xxx
        , a.more_info
        FROM wdadmin.wduser_people a,
        (SELECT c.people_id FROM 
        wdadmin.assessment_assessproject a,
        wdadmin.wduser_organization d,
        wdadmin.assessment_assessuser c
        where
        d.parent_id=%s
        and a.is_active=true
        and d.is_active=true
        and c.is_active=true
        and d.assess_id=a.id
        and a.id=%s
        and d.name<>'试测'
        and c.assess_id=a.id) b
        where a.is_active=true and a.id=b.people_id
        limit 1000
        """
    res = conn.get_data(sql, parentid, wduser_pid)
    return res  # id、username as xxx、more_info


@try_catch
def filter_rows(lst, name):
    return [i for i in lst if i[2]]


# json and 人员基础信息合计
@try_catch
def statistics_base_info(rows, name):
    res = []
    proper_key = []
    for i in rows:
        dict_lists = json.loads(i[2])

        for dic in dict_lists:
            property_key = dic.get("key_name")
            # if property_key in [u"年龄", u"性别", u"岗位序列", u"司龄", u"层级"]:
            property_value = dic.get("key_value")
            res.append([i[0], i[1], property_key, property_value])
            if property_key not in proper_key:
                proper_key.append(property_key)
    return res


# 转置 and Select values 3   ** people_id, username,...
@try_catch
def transpose(lst, name):
    res = {}
    target = [u"年龄", u"性别", u"岗位序列", u"司龄", u"层级"]
    global profile
    profile = []
    for i in lst:  # [people_id, username, property_key, property_value]
        key = "%s-%s" % (unicode(i[0]), i[1])
        if key not in res:
            res[key] = [i[0], i[1]]
        else:
            if i[2] in target:
                res[key].append(i[-1])
                if i[2] not in profile:
                    profile.append(i[2])
    return [res[j] for j in res]


# 组织人员关系  ** people_id, org_code
@try_catch
def people_relationship(conn, name):
    sql = """
            SELECT
            people_id
            , org_code
            FROM wduser_peopleorganization a,
            (SELECT max(id) id from wduser_peopleorganization
            where is_active=true
            group by people_id) b 
            where a.id=b.id
            order by people_id,org_code
            limit 225200,1000
        """
    res = conn.get_data(sql)
    return res


# Merge Join 3 2  inner join
@try_catch
def merge_inner(left, right, lindex, rindex, name):
    res = []
    for i in left:
        i = list(i)
        for j in right:
            if i[lindex] == j[rindex]:
                j = list(j)
                i.extend(j)
                res.append(i)
    return res


def line1(conn, parentid, wduser_pid):
    base_info = people_base_info(conn, parentid, wduser_pid, name=u"人员基础信息")
    # Filter rows
    filter_row = filter_rows(base_info, name=u"过滤more_info为空的数据")
    statistic_base_info = statistics_base_info(filter_row, name=u"合计人员基础信息")
    # 排序
    statistic_base_info.sort(key=lambda x: (x[0], x[1]))
    # 转置
    select_values_3 = transpose(statistic_base_info, name=u"转置、Select values 3")
    select_values_3.sort(key=lambda x: x[0])
    # 组织人员关系
    people_relation = people_relationship(conn, name=u"组织人员关系")
    # Merge Join 3 2
    merge_join_32 = merge_inner(select_values_3, people_relation, 0, 0, name=u"Merge Join 3 2")
    # sort
    merge_join_32.sort(key=lambda x: (x[-1], x[0]))
    return merge_join_32


# ***************机构一览表************
# 机构一览表
@try_catch
def list_org(conn, aid, name):  # orgname, orgcode
    sql = """
        select GetAncestry(id) orgname,identification_code org_code from wduser_organization
        where is_active=true and assess_id=%s
        limit 100
        """
    res = conn.get_data(sql, aid)
    return res


# Split fields 2
@try_catch
def split_field_2(orgs, name):
    res = []
    for org in orgs:
        org_list = org[0].split(",")
        if len(org_list) < 9:
            length = 9 - len(org_list)
            org_list.extend([None]*length)
        org_list.append(org[1])
        res.append(org_list)
    return res


def line2(conn, aid):  # [org1,org2,...org9, org_code]
    orgs = list_org(conn, aid, name=u"机构一览表")
    split_field = split_field_2(orgs, name="split field 2")
    split_field.sort(key=lambda x: x[-1])
    return split_field


# Select values 2 and Sort rows 4 2
@try_catch
def select_values_2(merge_value, name):
    for i in merge_value:
        i.pop(7)
        i.pop(7)
        i.pop(-1)
    merge_value.sort(key=lambda x: x[0])
    return merge_value


# ***************人员列表2 答题时间************
# 人员列表2 同 人员列表

# Select values 4 2
@try_catch
def select_value_4_2(arg, length, name):
    res = []
    for lst in arg:
        ret = lst[:-(length/2)]
        ret.pop(16)
        res.append(ret)

    tag_value = arg[0][-(length/2):]
    return res, tag_value


@try_catch
def answer_time(conn, sur_id, name):
    sql = """
        SELECT
        people_id, question_id, max(answer_time) as answer_time
        FROM front_userquestionanswerinfo a,
        (select max(id) id FROM front_userquestionanswerinfo 
        where project_id=191 and survey_id=%s and is_active=true
        group by people_id,question_id,answer_id
        ) b
        where a.id=b.id
        group by people_id,question_id
        order by people_id,question_id
        limit 100
        """
    res = conn.get_data(sql, sur_id)
    return res


@try_catch
def select_value_5(merge5, name):
    res = []
    for i in merge5:
        res.append([i[0], i[2], i[-1]])
    return res


def line4(conn, pro_id, sur_id, t_id):
    people_id_tuples = list_people(conn, pro_id, sur_id, name=u"数据库查询人员列表")
    answerTime = answer_time(conn, sur_id, name=u"统计答题时间")
    # Merge join 2 2  return: people_id, question_id, answer_time, people_id
    merge_join_2_2 = merge_left(answerTime, people_id_tuples, 0, 0, name=u"合并人员列表和答题时间")
    merge_join_2_2.sort(key=lambda x: x[0])
    # 题目编号 2  return: object_id, tag_value
    question_tag_tuples = question_tag(conn, t_id, name=u"数据库查询题目编号")
    # Merge join 5
    # return: people_id, question_id, answer_time, people_id, object_id, tag_value
    merge_join_5 = merge_left(merge_join_2_2, question_tag_tuples, 1, 0, name="Merge join 5")
    # Select values 5  ret: people_id, answer_score, tag_value
    select_values_5 = select_value_5(merge_join_5, name="Select values 5")
    # Sort rows 4 4 2
    select_values_5.sort(key=lambda x: (x[0], x[-1]))
    # Row denormaliser 3
    rowDenormaliser3 = row_denormaliser(select_values_5, name=u"列转行")
    denormaliser_item_len = len(rowDenormaliser3[0])
    select_values_4_2_3 = select_value_4_2(rowDenormaliser3, denormaliser_item_len)[0]
    # Sort rows 5 2
    select_values_4_2_3.sort(key=lambda x: x[0])
    # sort_rows_5_2 = select_values_4_2_3
    return select_values_4_2_3


# Select values 4 2 2 2
@try_catch
def select_value_4222(arg, leng, name):
    res = []
    for item in arg:
        res.append(item[leng+1:])
    return res


if __name__ == '__main__':
    HOST = "rm-bp1i2yah9e5d27k26bo.mysql.rds.aliyuncs.com"
    PORT = 3306
    DB = "wdadmin"
    user = "ad_wd"
    pwd = "Admin@Weidu2018"
    sql_conn = MySqlConn(HOST, PORT, DB, user, pwd, )
    DB_front = "wdfront"
    front_conn = MySqlConn(HOST, PORT, DB_front, user, pwd,)

    u'''人员基础信息'''
    wduser_people_id = 191
    parent_id = 0
    # Sort rows 3
    # people_id, username, property_key1,  ...property_key5,  people_id org_code
    sort_rows_3 = line1(sql_conn, parent_id, wduser_people_id)

    u'''机构一览表'''
    assess_id = 191

    # org1, org2, ..., org9,org_code
    sort_rows_2 = line2(sql_conn, assess_id)
    # Merge join 3
    # people_id, username, property_key1,  ...property_key5,  people_id org_code---# org1, org2, ..., org9,org_code
    merge_join_3 = merge_left(sort_rows_3, sort_rows_2, -1, -1, name=u"Merge_join_3")
    # Select values 2
    json.dump(merge_join_3, open("merge_join_3.json", "w"))
    sort_select_values_2 = select_values_2(merge_join_3, name="select_values_2")
    u'''人员列表、问卷答案'''
    project_id = 191
    survey_id = 132
    tag_id = 54
    # Sort rows 4
    sort_rows_4 = line3(front_conn, project_id, survey_id, tag_id, sql_conn)
    u'''Merge Join 3 3'''
    merge_join_3_3 = merge_left(sort_select_values_2, sort_rows_4, 0, 0, name=u"Merge Join 3 3")
    # Select values 4 2

    sort_rows_4_item_length = len(sort_rows_4[0])
    time.sleep(0.1)

    # select_values_4_2 = merge_join_3_3[:-sort_rows_4_item_length] + merge_join_3_3[-sort_rows_4_item_length/2:]
    select_values_4_2, tag_value = select_value_4_2(merge_join_3_3, sort_rows_4_item_length, name="select value 4 2")
    # Sort rows 5 2 2

    select_values_4_2.sort(key=lambda x: x[0])
    u"""
    '''line4'''
    sort_rows_5_2 = line4(sql_conn, project_id, survey_id, tag_id)
    # ***********Merge Join 6***********
    merge_join_6 = merge_left(select_values_4_2, sort_rows_5_2, 0, 0, name="Merge Join 6")
    """
    # ***********Select values 4 2 2 2***********
    # length = len(select_values_4_2[0])
    # selectValue_4222 = select_value_4222(merge_join_6, length)
    selectValue_4222 = select_values_4_2
    # Text file output

    # profile = [年龄,性别,司龄,层级,岗位序列]，transpose() global
    # tag_value = [C,.....S,....]
    column_index = ["peopple_id", u"姓名"] + profile + ["organization%d" % n for n in xrange(1, 10)] + tag_value

    sql_conn.close()



