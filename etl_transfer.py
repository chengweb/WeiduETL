# -*- coding:utf-8 -*-
import json
import time
import logging
import re
import sys
import pandas as pd
import pymysql
import functools
reload(sys)
sys.setdefaultencoding('utf8')


def logger_(name):
    log = logging.getLogger()

    fh = logging.FileHandler(name)
    ch = logging.StreamHandler()

    fm = logging.Formatter('%(levelname)s %(asctime)s %(filename)s %(lineno)d %(message)s')
    fh.setFormatter(fm)
    ch.setFormatter(fm)

    if log.handlers:
        log.handlers.pop()

    log.addHandler(fh)
    log.addHandler(ch)
    log.setLevel('INFO')

    return log


logger = logger_("sql_transfer.log")


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
            logger.error(u"执行%s出错" % name)
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
        """
    res = ['people_id'], conn.get_data(sql, pro_id, sur_id)
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
        """
    res = conn.get_data(sql, pro_id, sur_id)
    # ((people_id, question_id, sum(answer_score)), (people_id, question_id, sum(answer_score)), ...)
    return ['people_id', 'question_id', 'answer_score'], res


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
        """
    res = conn.get_data(sql, t_id)
    return ['object_id', 'tag_value'], res  # ((object_id, tag_value), )


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
        tag = lst[2]
        # tagValue合法性
        re_rule = r"G\d{1,2}|S\d{1,2}|C\d{1,2}|R\d{1,2}|L\d{1,2}|Z\d{1,2}|X\d{1,2}|BENM\d{1,2}|MASK\d{1,2}|N\d{1,2}"

        if re.match(re_rule, tag):
            if key not in res:
                res[key] = {"score": [], "tag_value": []}
            else:
                res[key]["score"].append(lst[1])
                res[key]["tag_value"].append(lst[2])
    ret = []
    colIndex = None
    for pid in res:
        line = [pid]
        line.extend(res[pid]["score"])
        # line.extend(res[pid]["tag_value"])
        ret.append(line)
        if not colIndex:
            colIndex = ['people_id'] + res[pid]["tag_value"]
    return colIndex, ret


# ***************人员列表、问卷答案************
def line3(conn, pro_id, sur_id, t_id, *args):
    admin_conn = args[0]
    # 人员列表 ((people_id, ), (people_id, ), ...)
    column_index_pit, people_id_tuples = list_people(conn, pro_id, sur_id, name=u"数据库查询人员列表")
    # 问卷答案 ((people_id, question_id, answer_score), ...)
    ''''''
    column_index_at, answer_tuples = get_answer(conn, pro_id, sur_id, name=u"数据库查询问卷答案")

    column_index_mj2, merge_join_2 = merge(answer_tuples, people_id_tuples, column_index_at, column_index_pit,
                         how='left', on=['people_id'], )

    # Sort rows 4 3
    merge_join_2.sort(key=lambda x: x[1])
    column_index_qt, question_tag_tuples = question_tag(admin_conn, t_id, name=u"数据库查询题目编号")
    column_index_mj, merge_join= merge(merge_join_2, question_tag_tuples, column_index_mj2, column_index_qt,
                      left_on=['question_id'], right_on=['object_id'], del_column=['question_id', 'object_id'])
    # Select values
    select_value = merge_join
    column_index_sv = ['people_id', 'answer_score', 'tag_value']
    select_value.sort(key=lambda x: (x[0], x[2]))
    column_index_rd, RowDenormaliser = row_denormaliser(select_value, name=u"列转行")
    # Sort rows 4
    RowDenormaliser.sort(key=lambda x: x[0])
    sort_rows_4 = RowDenormaliser
    return column_index_rd, sort_rows_4


# @try_catch
def people_base_info(conn, aid, name):
    sql = """
        SELECT
          a.id
        , a.username as xxx
        , a.more_info
        FROM wduser_people a,
        (SELECT c.people_id FROM 
        assessment_assessproject a,
        assessment_assessuser c
        where c.is_active=true
        and a.id=%s
        and c.assess_id=a.id) b
        where a.is_active=true and a.id=b.people_id
        """
    res = conn.get_data(sql, aid)
    return ['id', 'xxx', 'more_info'], res  # id、username as xxx、more_info


@try_catch
def filter_rows(lst, name):
    return [i for i in lst if i[2]]


@try_catch
def statistics_base_info(rows, name):
    res = []
    proper_key = []
    for i in rows:
        dict_lists = json.loads(i[2])

        for dic in dict_lists:
            property_key = dic.get("key_name")
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
    profile.insert(0, "people_id")
    profile.insert(1, "username")
    return profile, [res[j] for j in res]


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
        """
    res = conn.get_data(sql)
    return ["people_id", "org_code"], res


def del_repeat(arg):
    res = []
    for i in arg:
        if i not in res:
            res.append(i)
    return res


def merge(left, right, lindex, rindex, how='inner', on=None, left_on=None, right_on=None, del_column=None, axis=1):
    # 转DataFrame对象，设置列索引
    if not isinstance(left, list):
        left = list(left)
    if not isinstance(right, list):
        right = list(right)
    pd_left = pd.DataFrame(left)
    pd_left.columns = lindex

    pd_right = pd.DataFrame(right)
    pd_right.columns = rindex
    # merge
    result = pd.merge(pd_left, pd_right, how=how, on=on, left_on=left_on, right_on=right_on)
    result = result.drop_duplicates()  # 去重
    if del_column:  # 删除列
        result = result.drop(del_column, axis=axis)
    # 处理NaN
    result = result.where(result.notnull(), None)

    return result.columns.tolist(), result.values.tolist()


def line1(conn, aid):
    column_index, base_info = people_base_info(conn, aid, name=u"人员基础信息")
    # Filter rows
    filter_row = filter_rows(base_info, name=u"过滤more_info为空的数据")
    statistic_base_info = statistics_base_info(filter_row, name=u"合计人员基础信息")
    # 排序
    statistic_base_info.sort(key=lambda x: (x[0], x[1]))
    # 转置 and Select values 3  ********************
    column_index_sv3, select_values_3 = transpose(statistic_base_info, name=u"转置、Select values 3")

    select_values_3.sort(key=lambda x: x[0])
    # 组织人员关系  *******************
    column_index_pr, people_relation = people_relationship(conn, name=u"组织人员关系")
    # Merge Join 3 2  ***************
    column_index_mj_32, merge_join_32 = merge(select_values_3, people_relation, lindex=column_index_sv3, rindex=column_index_pr, on=["people_id"])
    # sort
    merge_join_32.sort(key=lambda x: (x[-1], x[0]))
    return column_index_mj_32, merge_join_32


# ***************机构一览表************
# 机构一览表
@try_catch
def list_org(conn, aid, name):  # orgname, orgcode
    sql = """
        select GetAncestry(id) orgname,identification_code org_code from wduser_organization
        where is_active=true and assess_id=%s
        """
    res = conn.get_data(sql, aid)
    return ['orgname', 'org_code'], res


# Split fields 2
@try_catch
def split_field_2(orgs, name):
    res = []
    for org in orgs:
        org_list = org[0].split(",")
        if len(org_list) < 9:
            length = 9 - len(org_list)
            org_list.extend([None] * length)
        org_list.append(org[1])
        res.append(org_list)
    return res


def line2(conn, aid):  # [org1,org2,...org9, org_code]
    column_index_orgs, orgs = list_org(conn, aid, name=u"机构一览表")
    split_field = split_field_2(orgs, name="split field 2")
    # column_index_orgs = ('orgname', 'org_code'), 转换方式固定
    orgs_index = [u'一级机构',
                  u'二级机构',
                  u'三级机构',
                  u'四级机构',
                  u'五级机构',
                  u'六级机构',
                  u'七级机构',
                  u'八级机构',
                  u'九级机构',
                  'org_code'
                  ]
    split_field.sort(key=lambda x: x[-1])
    return orgs_index, split_field


# ***************人员列表2 答题时间************
# 人员列表2 同 人员列表

# Select values 4 2
@try_catch
def select_value_4_2(arg, length, name):
    res = []
    for lst in arg:
        ret = lst[:-(length / 2)]
        ret.pop(16)
        res.append(ret)

    tag_value = arg[0][-(length / 2):]
    return res, tag_value


@try_catch
def select_value_5(merge5, name):
    res = []
    for i in merge5:
        res.append([i[0], i[2], i[-1]])
    return res


# Select values 4 2 2 2
@try_catch
def select_value_4222(arg, leng, name):
    res = []
    for item in arg:
        res.append(item[leng + 1:])
    return res


# 计算某组tag的索引
def index_colum(col_index, tag_list):
    indexs = []
    for tag in tag_list:
        try:
            tag_index = col_index.index(tag)
        except ValueError:
            tag_index = None
        indexs.append(tag_index)
    return indexs


# 根据tag组计算分数--算一个组的
# person_score_list  selectValue_4222的一个元素
# fields G1+G2
def compute_tag_group(person_score_list, col_index, fields, multiple):
    # person_list = list(set(person_list))
    field = fields.split("+")
    index_list = index_colum(col_index, field)
    score = 0

    for ind in index_list:
        if ind != None:
            try:
                score += person_score_list[ind]
            except TypeError:
                score += 0
    return score * multiple // len(field)


# 计算一个人的各项分数
def compute_person(person_score_list, col_index):
    res = {}
    fields_dict = {
        20: [
            'G1+G2', 'G3', 'G4', 'G5', 'G6+G7', 'G8',
            'S1', 'S2+S3', 'S4+S5', 'S6', 'S7+S8+S9',
            'C1+C2+C3+C4', 'C5+C6+C7', 'C8+C9+C10',
            'R1+R2', 'R3+R4', 'R5+R6', 'R7', 'R8',
            'L1+L2+L3', 'L4+L5', 'L6+L7+L8+L9+L10+L11+L12+L13', 'L14+L15',
            'L16+L17+L18', 'L19+L20',
            'Z1', 'Z2+Z3', 'Z4+Z5', 'Z6+Z7', 'Z8+Z9', 'Z10+Z11', 'Z12+Z13+Z14+Z15',
            'Z16+Z17', 'Z18+Z19', 'Z20+Z21+Z22+Z23',
            'X1+X2+X3+X4', 'X5+X6+X7+X8+X9', 'X10+X11+X12', 'X13+X14+X15', 'X16+X18+X17',
            'BENM1',
            'MASK1', 'MASK2', 'MASK3', 'MASK4',
        ],
        10: ['BENM2'],
        25: [
            'N1+N2', 'N3+N4', 'N5+N6', 'N7+N8', 'N9+N10', 'N11+N12', 'N13+N14',
            'N15+N16', 'N17+N18', 'N19+N20', 'N21+N22', 'N23+N24',
        ]
    }
    item = {'S7+S8+S9': u'\u8eab\u5fc3\u5173\u7231', 'N17+N18': u'\u4e50\u89c2\u79ef\u6781',
            'Z16+Z17': u'\u9f13\u52b1\u521b\u65b0', 'S1': u'\u85aa\u916c\u4fdd\u969c',
            'R3+R4': u'\u8de8\u754c\u534f\u4f5c',
            'S6': u'\u5bb6\u5ead\u5173\u7231', 'R1+R2': u'\u5171\u540c\u62c5\u5f53',
            'N7+N8': u'\u81ea\u6211\u62d3\u5c55',
            'C5+C6+C7': u'\u7cfb\u7edf\u57f9\u517b', 'C1+C2+C3+C4': u'\u6210\u957f\u8def\u5f84',
            'Z10+Z11': u'\u4fe1\u606f\u900f\u660e', 'X16+X18+X17': u'\u56e2\u961f\u6d3b\u529b',
            'R5+R6': u'\u5f00\u653e\u5305\u5bb9', 'N15+N16': u'\u5305\u5bb9\u5dee\u5f02',
            'G5': u'\u89d2\u8272\u6e05\u6670',
            'G4': u'\u6761\u4ef6\u652f\u6301', 'G3': u'\u73af\u5883\u8212\u9002', 'L4+L5': u'\u7cfb\u7edf\u628a\u63e1',
            'L16+L17+L18': u'\u91ca\u653e\u6f5c\u80fd', 'X13+X14+X15': u'\u656c\u4e1a\u6295\u5165',
            'N13+N14': u'\u4eb2\u548c\u5229\u4ed6', 'G8': u'\u5de5\u4f5c\u6210\u5c31',
            'G6+G7': u'\u4efb\u52a1\u5b89\u6392',
            'C8+C9+C10': u'\u6210\u957f\u673a\u5236', 'X1+X2+X3+X4': u'\u5de5\u4f5c\u72b6\u6001',
            'MASK4': u'\u63a9\u9970\u98984', 'Z1': u'\u613f\u666f\u6fc0\u53d1', 'G1+G2': u'\u5b89\u5168\u5065\u5eb7',
            'R7': u'\u4eba\u9645\u5438\u5f15', 'X10+X11+X12': u'\u7ec4\u7ec7\u8ba4\u540c',
            'Z18+Z19': u'\u6587\u5316\u4f18\u79c0', 'MASK1': u'\u63a9\u9970\u98981',
            'Z6+Z7': u'\u6388\u6743\u8d4b\u80fd',
            'R8': u'\u76f8\u4e92\u4fc3\u8fdb', 'L19+L20': u'\u4fc3\u8fdb\u6210\u957f', 'MASK3': u'\u63a9\u9970\u98983',
            'Z4+Z5': u'\u6d41\u7a0b\u9ad8\u6548', 'S2+S3': u'\u751f\u6d3b\u5e73\u8861',
            'Z2+Z3': u'\u7ec4\u7ec7\u914d\u7f6e',
            'S4+S5': u'\u798f\u5229\u5173\u7231', 'N1+N2': u'\u81ea\u4e3b\u5b9a\u5411',
            'Z20+Z21+Z22+Z23': u'\u54c1\u724c\u5f71\u54cd', 'MASK2': u'\u63a9\u9970\u98982',
            'L6+L7+L8+L9+L10+L11+L12+L13': u'\u6fc0\u53d1\u4fe1\u4efb', 'N3+N4': u'\u610f\u4e49\u5bfb\u6c42',
            'N23+N24': u'\u7075\u6d3b\u53d8\u901a', 'N11+N12': u'\u4e13\u6ce8\u6295\u5165',
            'L14+L15': u'\u8bfb\u61c2\u4ed6\u4eba', 'N5+N6': u'\u81ea\u6211\u60a6\u7eb3',
            'N21+N22': u'\u5408\u7406\u5f52\u56e0', 'N19+N20': u'\u81ea\u4fe1\u575a\u97e7',
            'X5+X6+X7+X8+X9': u'\u7559\u4efb\u503e\u5411', 'Z12+Z13+Z14+Z15': u'\u4ef7\u503c\u6fc0\u52b1',
            'L1+L2+L3': u'\u76ee\u6807\u5f15\u9886', 'BENM2': u'\u5e78\u798f\u5ea6',
            'BENM1': u'\u5c97\u4f4d\u538b\u529b',
            'N9+N10': u'\u60c5\u7eea\u8c03\u8282', 'Z8+Z9': u'\u7eb5\u5411\u652f\u6301'}

    for multiple in fields_dict:
        for fields in fields_dict[multiple]:
            score = compute_tag_group(person_score_list, col_index, fields, multiple)
            res[item[fields]] = score
    return res


# 按各项汇总
# items>>: list
def statistics(score):
    statistic = {}
    res = {
        u"个人幸福能力": [u"自主定向", u"意义寻求", u"自我悦纳", u"自我拓展", u"情绪调节", u"专注投入",
                    u"亲和利他", u"包容差异", u"乐观积极", u"自信坚韧", u"合理归因", u"灵活变通"],
        u"幸福效能": [u"工作状态", u"留任倾向", u"组织认同", u"敬业投入", u"团队活力"],
        u"工作投入": [u"安全健康", u"环境舒适", u"条件支持", u"角色清晰", u"任务安排", u"工作成就"],
        u"生活愉悦": [u"薪酬保障", u"生活平衡", u"福利关爱", u"家庭关爱", u"身心关爱"],
        u"成长有力": [u"成长路径", u"系统培养", u"成长机制"],
        u"人际和谐": [u"共同担当", u"跨界协作", u"开放包容", u"人际吸引", u"相互促进"],
        u"领导激发": [u"目标引领", u"系统把握", u"激发信任", u"读懂他人", u"释放潜能", u"促进成长"],
        u"组织卓越": [u"愿景激发", u"组织配置", u"流程高效", u"授权赋能", u"纵向支持",
                  u"信息透明", u"价值激励", u"鼓励创新", u"文化优秀", u"品牌影响"]
    }
    for i in res:  # i == u"个人幸福能力"
        statistic[i] = 0
        for field in res[i]:
            statistic[i] += score[field]
        statistic[i] = round(statistic[i] // len(res[i]), 2)
    statistic[u"幸福维度总分"] = round((statistic[u"工作投入"] + statistic[u"生活愉悦"] + statistic[u"成长有力"] +
                                  statistic[u"人际和谐"] + statistic[u"领导激发"] + statistic[u"组织卓越"]) // 6, 2)
    statistic[u"幸福总分"] = statistic[u"幸福维度总分"] * 0.8 + statistic[u"个人幸福能力"] * 0.2

    return statistic


# 计算所有分数
def compute_all(all_score, col_index):
    res = []
    for person_score_list in all_score:
        personal_res = {}
        for i in xrange(16):
            personal_res[col_index[i]] = person_score_list[i]
        score = compute_person(person_score_list, col_index)
        statistic = statistics(score)
        personal_res["score"] = score
        personal_res["statistics"] = statistic

        res.append(personal_res)

    return res


def main(AssessID, SurveyID):
    HOST = "rm-bp1i2yah9e5d27k26.mysql.rds.aliyuncs.com"
    PORT = 3306
    DB = "wdadmin"
    user = "ad_wd"
    pwd = "Admin@Weidu2018"
    sql_conn = MySqlConn(HOST, PORT, DB, user, pwd, )
    DB_front = "wdfront"
    front_conn = MySqlConn(HOST, PORT, DB_front, user, pwd, )

    assess_id = project_id = AssessID  # 191
    survey_id = SurveyID  # 132
    tag_id = 54  # 固定
    u'''人员基础信息'''
    # Sort rows 3
    column_index_sr3, sort_rows_3 = line1(sql_conn, assess_id)
    u'''机构一览表'''
    column_index_sr2, sort_rows_2 = line2(sql_conn, assess_id)

    column_index_mj3, merge_join_3 = merge(sort_rows_3, sort_rows_2, column_index_sr3, column_index_sr2,
                         how='left', on='org_code', del_column="org_code")
    # Select values 2
    sort_select_values_2 = merge_join_3
    column_index_mj3[column_index_mj3.index('username')] = u'姓名'
    u'''人员列表、问卷答案'''
    # Sort rows 4
    column_index_l3, sort_rows_4 = line3(front_conn, project_id, survey_id, tag_id, sql_conn)
    u'''Merge Join 3 3'''
    column_index_mj33, merge_join_3_3 = merge(sort_select_values_2, sort_rows_4, column_index_mj3, column_index_l3,
                                              how='left', on=["people_id"])
    json.dump(column_index_mj33, open('column_index_mj33.json', 'w'), ensure_ascii=False)
    # Select values 4 2
    json.dump(merge_join_3_3, open('merge_join_3_3.json', 'w'), ensure_ascii=False)
    result = compute_all(merge_join_3_3, column_index_mj33)
    json.dump(result, open("result.json", "w"), ensure_ascii=False)

    sql_conn.close()


if __name__ == '__main__':
    main(191, 132)
