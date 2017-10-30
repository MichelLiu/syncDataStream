# -*- coding: utf-8 -*-

# import random
# import pytz
# from datetime import datetime
# import time
import json
import time
import random

def randomStr(length):
    strRam = ""
    for i in range(0,length):
        strRam = strRam + str(random.randint(0,9))
    return strRam

eventdata = [
    {},
    {#股东信息[变更]
        "before":{ #变更前
            #股东名称
            "INV":"陈青山、罗希平、金连文、其实、" + randomStr(6),
        },
        "after":{  #变更后
            #股东名称
            "INV":"陈青山、罗希平、金连文、其实、龙腾、" + randomStr(6),
        }
    },
    {#股东出资[变更]
        "info": [
            {
                "before": {  # 变更前
                    # 股东名称
                    "INV": "镇立新",
                    # 认缴额
                    "SUBCONAM": "371.37",
                    # 实缴额
                    "ACCONAM": "126.3278",
                    # 单位
                    "REGCAPCUR": "万人民币"

                },
                "after": {  # 变更后
                    # 股东名称
                    "INV": "镇立新",
                    # 认缴额
                    "SUBCONAM": "5" + randomStr(2) + ".0",
                    # 实缴额
                    "ACCONAM": "2" + randomStr(2) + ".0",
                    # 单位
                    "REGCAPCUR": "万人民币"
                }
            },
            {
                "before": {  # 变更前
                    # 股东名称
                    "INV": "陈青山",
                    # 认缴额
                    "SUBCONAM": "1" + randomStr(2) + ".0",
                    # 实缴额
                    "ACCONAM": "2" + randomStr(1) + ".0",
                    # 单位
                    "REGCAPCUR": "万人民币"

                },
                "after": {  # 变更后
                    # 股东名称
                    "INV": "陈青山",
                    # 认缴额
                    "SUBCONAM": "3" + randomStr(2) + ".0",
                    # 实缴额
                    "ACCONAM": "2" + randomStr(2) + ".0",
                    # 单位
                    "REGCAPCUR": "万人民币"
                }
            }
        ]
    },
    {#主要人员[变更]
         "before":{ #变更前
                #主要人员与职务
                "INV":"黄国强、王艺谭、汤松榕、龙腾、" + randomStr(6),
         },
         "after":{  #变更后
                #主要人员与职务
                "INV":"黄国强、王艺谭、汤松榕、龙腾、镇立新、罗希平、陈青山、" + randomStr(6),
         }
    },
    {#工商变更[变更]
         "before":{ #变更前
                #Key：
                "注册资本变更":"1000.000000万人民币",
                #Key
                "董事备案":"镇立新;茹海波;黄国强;龙腾;陈青山;" + randomStr(6)
         },
         "after":{  #变更后
                  #Key
                "注册资本变更":"1"+ randomStr(3) + ".388900万人民币",
                #Key
                "董事备案":"龙腾;黄国强;陈青山;汤松榕;王艺潭;左凌烨;镇立新;" + randomStr(6)
         }
	},
    {#子公司与分支机构[新增]
        "ENTNAME":"上海合合信息科技发展有限公司上海分公司" + randomStr(6),#企业名称
		"ENTTYPE":"全资子公司",#企业类型
		"LEGALPERSON":"镇立新" + randomStr(6),#企业法人
		"LEGALPERSONDESC":"法定代表人",
		"RECCAP":"200.0万",#注册资本
		"REGCAPCUR":"人民币",#注册资本单位
		"ESDATE":"2008-08-08T00:00:00.000Z",#成立年限
		"ENTSTATUS":1,#企业状态
		"ENTSTATUSDESC": "存续（在营、开业、在册）",#企业状态
	},
    {#对外投资[新增]
		"ENTNAME":"苏州贝尔塔数据技术有限公司" + randomStr(6),#企业名称
		"ENTTYPE":"有限责任公司（自然人投资或控股）",#企业类型
		"PROVINCE":"31",#所在地：
		"LEGALPERSON":"镇立新" + randomStr(6),#企业法人
		"LEGALPERSONDESC":"法定代表人",
		"RECCAP":"",#注册资本
		"REGCAPCUR":"1061.388900万人民币",#注册资本单位
		"ESDATE":"2008-08-08T00:00:00.000Z",#成立年限
		"ENTSTATUS":1,#企业状态
		"ENTSTATUSDESC": "存续（在营、开业、在册）",#企业状态
		"AMOUNT":"800万元",#投资数额
		"SCALE":"80"#投资占比
	},
    {#企业年报[新增]
        # 报送年度
        "ANCHEYEAR": "2017",
        # 公示日期：
        "PUSH_DATE": "2017-01-01",
    },
    {#企业年报[变更]
        "before":{ #变更前
			#股东及出资信息：
			"SPONSOR":"陈青山、罗希平、金连文、其实、" + randomStr(6),
        },
        "after":{  #变更后
            #股东及出资信息：
            "SPONSOR":"陈青山、罗希平、金连文、其实、镇立新、" + randomStr(6),
        }
    },
    {#行政处罚
        "penDecNo": "杨市监案处字〔2016〕第13020161013" + randomStr(1) + "号",
        "illegActType": "虚假广告宣传",
        "penContent": "罚款80万元人民币",
        "penAuth_CN": "杨浦区市场监管局",
        "penDecIssDate": "2010-10-01T00:00:00.000Z",
        "publicDate": "2010-10-01T00:00:00.000Z"
	},
    {#经营异常[列入]
        "speCause_CN": "未准时上传年度报告",
        "abntime": "2017-07-01T00:00:00.000Z",
        "decOrg_CN": "杨浦区市场工商管理局",
    },
    {#经营异常[移出]
        "speCause_CN": "未准时上传年度报告",
        "abntime": "2017-07-01T00:00:00.000Z",
        "decOrg_CN": "杨浦区市场工商管理局",
        "remExcpRes_CN": "已上传年度报告",
        "remDate": "2017-08-0" + randomStr(1) + "T00:00:00.000Z",
        "reDecOrg_CN": "杨浦区市场工商管理局"
	},
    {#清算信息
        "info":[{
            "ligpriSign": "镇立新",  #清算组组长
			"liqMem": "陈青山、罗希平、金连文、其实、" + randomStr(6) #清算组成员
	}]},
    {#抽查信息
        "insAuth_CN": "杨浦区市场工商管理局",
        "insType_CN": "消防抽查",
        "insDate": "2017-07-0" + randomStr(1) + "T00:00:00.000Z",
        "insRes_CN": "正常"
	},
    {#司法协助
        "inv": "镇立新",
        "froAm": "1" + randomStr(2),
        "froAuth": "上海市中级人民法院",
        "executeNo": "（20015）上中法协执字第623号之二",
        "type": "股权冻结 丨 冻结"
	},
    {#欠税信息
        'CREATEDATE': "2017-07-01",#截止日期
        'TAXID': randomStr(13),#税号或统一码
        'TAXTYPE': "增值税",#欠税税种
        'TAXAMOUNT': "6"+ randomStr(4),#合计欠税金额
        'LAWERNAME': "张三四" + randomStr(1),#负责人姓名
        'LAWERCREDTYPE': "身份证",#证件名称
        'LAWERCREDID': "3" + randomStr(17),#证件号码
        'COMPANYNAME': "上海合合信息科技发展有限公司" + randomStr(6),#纳税人名称
	},
    {#税务非正常户
        'CREATEDATE':"2017-07-01",#截止日期
        'TAXID': randomStr(13),#税号或统一码
        'LAWERNAME': "上海合合信息科技发展有限公司" + randomStr(6),#法定代表人或负责人姓名
    },
	{#税务重大违法
        'CREATEDATE': "2017-07-01",                     #截止日期
        'TAXID': randomStr(13),                       #税号或统一码
        'CASEKIND': "民事案件",                         #案件类型
        'LAWERINFO': "张三分" + randomStr(6),                          #法定代表人或负责人姓名/性别/证件号码
        'MAINCASE': "不交税",                           #主要违法事件
        'PUNISHMENT': "罚款"                            #相关法律依据及税务处理处罚情况}],#税务重大
	},
    {#被执行人
        'FILINGDATE' : "2017-07-01",                      #立案时间
        'COURT' : "上海市中级人民法院",                   #执行法院
        'REFERENCENO' : "780",                            #案号
        'ENFOBJECT' : "标的",                             #执行标的
        'ENFNAME' : "张三分" + randomStr(6),             #被执行人姓名/名称
        'CARDNO' : "3" + randomStr(17),                  #身份证号码/组织机构代码
	},
    {#失信信息
        'REFERENCENO' : "（2017）冀0184执648号",                            #案号
        'ENFDUTY' : "判决1.被告上海逸讯机械制造有限公司立即停止侵犯原告杜景章“一种锅巴机（专利号ZL201220572874.0）实用新型专利权行为”。",#生效法律文书确定的义务
        'ENFSTATUS' : "全部未履行",                                      #失信被执行人行为具体情形
        'ENFNO' : "（2016）冀民终773号民事判决书",                       #执行依据文号
        'CARDNO' : "69724940-3",                                        #身份证号码/组织机构代码
        'PROVINCE' :"河北",                                                 #省份
        'PERSON' : "沈王舒",                                       #法定代表人或者负责人姓名
        'ENFNAME' : "上海合合信息科技发展有限公司" + randomStr(6),                     #被执行人姓名/名称
        'PUBDATE' :"2017年07月02日",                                     #发布时间
        'ENFSITUATION' :"其他妨碍、抗拒执行,其他规避执行",                 #被执行人的履行情况
        'COURT' : "新乐市人民法院",                                        #执行法院
        'FILINGDATE' :"2017年05月02日",                                    #立案时间
        'ENFUNIT' : "新乐市人民法院"                                     #做出执行依据单位
	},
    {#营业执照[变更]
        "before":{ #变更前
            #注册号
            "REGNO":randomStr(15),

            # 统一信用代码
            "UNCID":randomStr(18),
            # 企业(机构)类型
            "ENTTYPE": "有限责任公司（自然人投资或控股）",
              # 法人 经营者 负责人 投资人 执行事务合伙人
            "LEGALPERSON":"镇立新",
             # 成立日期
            "ESDATE": "2006-08-08T00:00:00.000Z",
             # 经营(驻在)期限自
            "OPFROM": "2006-08-08T00:00:00.000Z",

            # 经营(驻在)期限至
            "OPTO": "2035-06-06T00:00:00.000Z",
             # 核准日期
            "APPRDATE": "2006-08-08T00:00:00.000Z",
              #注册资金-字符类型
            "REGCAPSTR":"1061.388900",
             # 登记机关
            "REGORG": "杨浦区市场监管局",
              # 经营(业务)范围
            "OPSCOPE": "计算机及网络领域、人工智能技术领域内的技术开发、技术咨询、技术服务、技术转让；云平台服务和云软件服务；接受金融机构委托从事金融信息技术外包；接受金融机构委托从事金融业务流程外包；接受金融机构委托从事金融知识流程外包；硬件设备的设计、开发；数据的收集、处理、开发；企业征信服务，广告设计、制作，利用自有媒体发布广告；商务信息咨询，投资咨询，企业管理咨询（以上咨询除经纪），会务服务，企业营销策划，文化艺术策划；日用百货、电子产品、工艺美术品、数码产品的开发、销售；从事货物及技术的进出口业务。 【依法须经批准的项目，经相关部门批准后方可开展经营活动】",
        },
        "after":{  #变更后
           #注册号
            "REGNO":randomStr(15),

            # 统一信用代码
            "UNCID":randomStr(18),
            # 企业(机构)类型
            "ENTTYPE": "有限责任公司（自然人投资或控股）",
              # 法人 经营者 负责人 投资人 执行事务合伙人
            "LEGALPERSON":"镇立新",
             # 成立日期
            "ESDATE": "2006-08-08T00:00:00.000Z",
             # 经营(驻在)期限自
            "OPFROM": "2006-08-08T00:00:00.000Z",

            # 经营(驻在)期限至
            "OPTO": "",
             # 核准日期
            "APPRDATE": "2006-08-08T00:00:00.000Z",
              #注册资金-字符类型
            "REGCAPSTR":"1061.388900",
             # 登记机关
            "REGORG": "杨浦区市场监管局",
              # 经营(业务)范围
            "OPSCOPE": "计算机及网络领域、人工智能技术领域内的技术开发、技术咨询、技术服务、技术转让；云平台服务和云软件服务；接受金融机构委托从事金融信息技术外包；接受金融机构委托从事金融业务流程外包；接受金融机构委托从事金融知识流程外包；硬件设备的设计、开发；数据的收集、处理、开发；企业征信服务，广告设计、制作，利用自有媒体发布广告；商务信息咨询，投资咨询，企业管理咨询（以上咨询除经纪），会务服务，企业营销策划，文化艺术策划；日用百货、电子产品、工艺美术品、数码产品的开发、销售；从事货物及技术的进出口业务。 【依法须经批准的项目，经相关部门批准后方可开展经营活动】",
        }
    },
    {#开庭公告
        'FILINGDATE':"2017-07-01",                        #立案时间
        'COURT' :  "上海市中级人民法院",                   #法院
        'TRIBUNAL' :  "第一法庭",                         #法庭
        'COURTDATE' : "2017-06-01",                       #开庭日期
        'SCHEDULEDATE' :"2017-05-01",                     #排期日期
        'REFERENCENO' : "1232456576",                     #案号
        'DEPARTMENT' :"上海法院",                         #承办部门
        'JUDGE' :"李四",                                  #审判长/主审人
        'ACCUSER' : "上海合合信息科技发展有限公司" + randomStr(6),       #原告
        'DEFENDANT' : "老王",                             #被告
        'PARTY':"上海合合信息科技发展有限公司,老王",       #当事人
        'BRIEF':"不可描述"                           #案由
	},
    {#法院公告
        'ANNOTPYE': "开庭公告",                           #公告类型
        'ANNOER':"上海合合信息科技发展有限公司" + randomStr(6),       #公告人
        'LAWERNAME':"上海合合信息科技发展有限公司",      #当事人
        'ANNODATE': "2017-07-01",                        #公告时间
        'CONTENT': "不可描述",                            #正文内容
        'PUBLISHED':"G6",                                 #刊登版面
        'PUBLISHDATE': "2017-07-01",                     #刊登日期
        'UPLOADDATE': "2017-07-01",                      #上传日期
	},
    {#法院判决
        'CASETYPE': "刑事案件",                                         #案件类型  1=刑事案件, 2=民事案件, 3=行政案件, 4=赔偿案件, 5=执行案件, 6=民族语言文书
        'REFEREEDATE':"2017-07-01",                                     #裁判日期
        'CASENAME': "上海合合信息科技发展有限公司与老王的民事纠纷" + randomStr(6),     #案件名称
        'WRITID' :"abcdedefa-dfefa-fdaefae",                             #文书ID
        'PROCEDURE': "二审",                                            #审判程序
        'CASENO': "dfeadf" + randomStr(4),                                         #案号
        'COURTNAME': "上海市中级人民法院",                               #法院名称
        'JUDGMENTCONTENT':"上海合合信息科技发展有限公司与老王的民事纠纷维持原判", #判决内容
        'RELEASEDATE': "2017-07-01",                                    #发布日期
	},
    {#严重违法失信企业[列入]
        "type": "严重违法失信企业名单",
        "serILLRea_CN": "提交虚假材料或者采取其他欺诈手段隐瞒重要事实，取得公司变更或者注销登记，被撤销登记的" + randomStr(4),
        "abntime": "2017-07-01T00:00:00.000Z",
        "decOrg_CN": "上海市工商行政管理局",
	},
    {#严重违法失信企业[移出]
        "type": "严重违法失信企业名单",
        "serILLRea_CN": "提交虚假材料或者采取其他欺诈手段隐瞒重要事实，取得公司变更或者注销登记，被撤销登记的" + randomStr(4),
        "abntime": "2017-07-01T00:00:00.000Z",
        "decOrg_CN": "上海市工商行政管理局",
        "remExcpRes_CN": "已注销",
        "remDate": "2017-08-01T00:00:00.000Z",
        "reDecOrg_CN": "上海市工商行政管理局"
	},
]

eventType = [
    "营业执照[变更]",
    "股东信息[变更]",
    "出资信息[变更]",
    "主要人员[变更]",
    "对外投资[新增]",
    "行政处罚",
    "经营异常[列入]",
    "清算信息",
    "抽查信息",
    "司法协助",
    "欠税信息",
    "税务非正常户",
    "税务重大违法",
    "被执行人",
    "失信信息",
    "开庭公告",
    "法院公告",
    "法院判决",
    "司法违法失信企业[列入]",
    "营业执照信息[变更]",
    "工商变更[变更]",
    "子公司与分支机构[新增]",
    "企业年报[变更]",
    "企业年报[新增]",
    "经营异常[移出]",
    "司法违法失信企业[移出]",
]

def generateData(company, ttype):
    msg = {
        "event_name": eventType[ttype],
        "event_content": eventdata[ttype],
        "pid": pid,
        "entname":company,
        "happen_date": int(round(time.time() * 1000)),
        "type": ttype
    }
    return json.dumps(msg,ensure_ascii=False)


def main():
    print randomStr(9)

if __name__ == "__main__":
    main()