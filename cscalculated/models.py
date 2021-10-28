from sqlalchemy import Column, Float, String, Integer,Boolean, ForeignKey,_and
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine,MetaData  

db = create_engine('postgresql+psycopg2://postgres:common@localhost:5432')
connection = db.connect()
metadata = MetaData(db)

Base = declarative_base()

class PCC_GBUAGG_DATA(Base):
    __tablename__ = 'PCC_GBUAGG_DATA'
    id = Column(Integer, primary_key=True, autoincrement=False)
    TOT_ACC_FORBAR = Column(Integer, nullable=False, default=0)
    TOT_ACC_FORTERM = Column(Integer, nullable=False, default=0)
    CNT_ACTIVE_ACC = Column(Integer, nullable=False, default=0)
    CNT_DQ_ACC = Column(Integer, nullable=False, default=0)
    SUM_PROMISEAMT_GBU = Column(Float, nullable=False, default=0)
    CNT_BARRED_ACC = Column(Integer, nullable=False, default=0)
    CNT_HCA_ACC = Column(Integer, nullable=False, default=0)
    CNT_ADA_ARR = Column(Integer, nullable=False, default=0)
    CNT_VIP = Column(Integer, nullable=False, default=0)
    CNT_ACTIVESUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_BARREDSUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_SUBSCFORTERM_CC = Column(Integer, nullable=False, default=0)
    CNT_TERMSUBSC_CC = Column(Integer, nullable=False, default=0)
    ACCT_DQRATE = Column(Float, nullable=False, default=0)
    TOT_AR_DQRATE = Column(Float, nullable=False, default=0)
    ACTIVE_AR_DQRATE = Column(Float, nullable=False, default=0)
    TOT_BAL_DQ_ACC = Column(Integer, nullable=False, default=0)
    BUCKET1_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET2_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET3_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET4_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET5_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET6_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET7_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET8_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET9_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET10_AMOUNT = Column(Integer, nullable=False, default=0)

class PCC_COMPGAGG_DATA(Base):
    __tablename__ = 'PCC_COMPGAGG_DATA'
    id = Column(Integer, primary_key=True, autoincrement=False)
    TOT_ACC_FORBAR = Column(Integer, nullable=False, default=0)
    TOT_ACC_FORTERM = Column(Integer, nullable=False, default=0)
    CNT_ACTIVE_ACC = Column(Integer, nullable=False, default=0)
    CNT_DQ_ACC = Column(Integer, nullable=False, default=0)
    SUM_PROMISEAMT_GBU = Column(Float, nullable=False, default=0)
    CNT_BARRED_ACC = Column(Integer, nullable=False, default=0)
    CNT_HCA_ACC = Column(Integer, nullable=False, default=0)
    CNT_ADA_ARR = Column(Integer, nullable=False, default=0)
    CNT_VIP = Column(Integer, nullable=False, default=0)
    CNT_ACTIVESUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_BARREDSUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_SUBSCFORTERM_CC = Column(Integer, nullable=False, default=0)
    CNT_TERMSUBSC_CC = Column(Integer, nullable=False, default=0)
    ACCT_DQRATE = Column(Float, nullable=False, default=0)
    TOT_AR_DQRATE = Column(Float, nullable=False, default=0)
    ACTIVE_AR_DQRATE = Column(Float, nullable=False, default=0)
    TOT_BAL_DQ_ACC = Column(Integer, nullable=False, default=0)
    BUCKET1_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET2_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET3_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET4_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET5_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET6_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET7_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET8_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET9_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET10_AMOUNT = Column(Integer, nullable=False, default=0)

class PCC_ENTITYAGG_DATA(Base):
    __tablename__ = 'PCC_ENTITYAGG_DATA'
    id = Column(Integer, primary_key=True, autoincrement=False)
    TOT_ACC_FORBAR = Column(Integer, nullable=False, default=0)
    TOT_ACC_FORTERM = Column(Integer, nullable=False, default=0)
    CNT_ACTIVE_ACC = Column(Integer, nullable=False, default=0)
    CNT_DQ_ACC = Column(Integer, nullable=False, default=0)
    SUM_PROMISEAMT_GBU = Column(Float, nullable=False, default=0)
    CNT_BARRED_ACC = Column(Integer, nullable=False, default=0)
    CNT_HCA_ACC = Column(Integer, nullable=False, default=0)
    CNT_ADA_ARR = Column(Integer, nullable=False, default=0)
    CNT_VIP = Column(Integer, nullable=False, default=0)
    CNT_ACTIVESUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_BARREDSUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_SUBSCFORTERM_CC = Column(Integer, nullable=False, default=0)
    CNT_TERMSUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_ACTIVE_SUBSC_IC = Column(Integer, nullable=False, default=0)
    CNT_BARREDSUBSC_IC = Column(Integer, nullable=False, default=0)
    CNT_TERMSUBSC_IC = Column(Integer, nullable=False, default=0)
    ACCT_DQRATE = Column(Float, nullable=False, default=0)
    TOT_AR_DQRATE = Column(Float, nullable=False, default=0)
    ACTIVE_AR_DQRATE = Column(Float, nullable=False, default=0)
    TOT_BAL_DQ_ACC = Column(Integer, nullable=False, default=0)
    BUCKET1_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET2_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET3_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET4_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET5_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET6_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET7_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET8_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET9_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET10_AMOUNT = Column(Integer, nullable=False, default=0)


class PCC_CORPAGG_DATA(Base):
    __tablename__ = 'PCC_CORPAGG_DATA'
    id = Column(Integer, primary_key=True, autoincrement=False)
    TOT_ACC_FORBAR = Column(Integer, nullable=False, default=0)
    TOT_ACC_FORTERM = Column(Integer, nullable=False, default=0)
    CNT_ACTIVE_ACC = Column(Integer, nullable=False, default=0)
    CNT_DQ_ACC = Column(Integer, nullable=False, default=0)
    SUM_PROMISEAMT_GBU = Column(Float, nullable=False, default=0)
    CNT_BARRED_ACC = Column(Integer, nullable=False, default=0)
    CNT_HCA_ACC = Column(Integer, nullable=False, default=0)
    CNT_ADA_ARR = Column(Integer, nullable=False, default=0)
    CNT_VIP = Column(Integer, nullable=False, default=0)
    CNT_ACTIVESUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_BARREDSUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_SUBSCFORTERM_CC = Column(Integer, nullable=False, default=0)
    CNT_TERMSUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_ACTIVE_SUBSC_IC = Column(Integer, nullable=False, default=0)
    CNT_BARREDSUBSC_IC = Column(Integer, nullable=False, default=0)
    CNT_TERMSUBSC_IC = Column(Integer, nullable=False, default=0)
    ACCT_DQRATE = Column(Float, nullable=False, default=0)
    TOT_AR_DQRATE = Column(Float, nullable=False, default=0)
    ACTIVE_AR_DQRATE = Column(Float, nullable=False, default=0)
    TOT_BAL_DQ_ACC = Column(Integer, nullable=False, default=0)
    BUCKET1_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET2_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET3_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET4_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET5_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET6_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET7_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET8_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET9_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET10_AMOUNT = Column(Integer, nullable=False, default=0)

    
class PCC_CUSTAGG_DATA(Base):
    __tablename__ = 'PCC_CUSTAGG_DATA'
    id = Column(Integer, primary_key=True, autoincrement=False)
    TOT_ACC_FORBAR = Column(Integer, nullable=False, default=0)
    TOT_ACC_FORTERM = Column(Integer, nullable=False, default=0)
    CNT_ACTIVE_ACC = Column(Integer, nullable=False, default=0)
    CNT_DQ_ACC = Column(Integer, nullable=False, default=0)
    SUM_PROMISEAMT_GBU = Column(Float, nullable=False, default=0)
    CNT_BARRED_ACC = Column(Integer, nullable=False, default=0)
    CNT_HCA_ACC = Column(Integer, nullable=False, default=0)
    CNT_ADA_ARR = Column(Integer, nullable=False, default=0)
    CNT_VIP = Column(Integer, nullable=False, default=0)
    CNT_ACTIVESUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_BARREDSUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_SUBSCFORTERM_CC = Column(Integer, nullable=False, default=0)
    CNT_TERMSUBSC_CC = Column(Integer, nullable=False, default=0)
    CNT_ACTIVE_SUBSC_IC = Column(Integer, nullable=False, default=0)
    CNT_BARREDSUBSC_IC = Column(Integer, nullable=False, default=0)
    CNT_TERMSUBSC_IC = Column(Integer, nullable=False, default=0)
    ACCT_DQRATE = Column(Float, nullable=False, default=0)
    TOT_AR_DQRATE = Column(Float, nullable=False, default=0)
    ACTIVE_AR_DQRATE = Column(Float, nullable=False, default=0)
    TOT_BAL_DQ_ACC = Column(Integer, nullable=False, default=0)
    BUCKET1_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET2_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET3_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET4_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET5_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET6_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET7_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET8_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET9_AMOUNT = Column(Integer, nullable=False, default=0)
    BUCKET10_AMOUNT = Column(Integer, nullable=False, default=0)

class PCC_Account(Base):
    __tablename__ = 'PCC_Account'
    id = Column(Integer, primary_key=True, autoincrement=False)
    collection_status = Column(String(50),nullable=False) #1-2,6,12 Barring Requested,Termination Requested \
                                                                #For Termination, Barred,Termination 
    account_status = Column(String(50),nullable=False) #3,19 Active
    delinquent_account = Column(Boolean,nullable=False) #4,17,20 'Y'
    on_hold_flag = Column(String(50),nullable=False) #7
    pay_means = Column(String(50),nullable=False)  #8 Auto Debit
    outstanding_balance = Column(Float,nullable=False,default=0)#18,20
    
    GBU = Column(String(50),nullable=False)
    Custom_Group = Column(String(50),nullable=False)
    Legal_Entity = Column(String(50),nullable=False)
    
    GBU_ID = Column(String(50),nullable=False)
    Group_ID = Column(String(50),nullable=False)
    Entity_ID = Column(String(50),nullable=False)
    Corp_ID = Column(String(50),nullable=False)
    Customer_ID = Column(String(50),nullable=False)
    
    
class PCC_Arrangement(Base):
    __tablename__ = 'PCC_Arrangement'
    id = Column(Integer, primary_key=True, autoincrement=False)
    isonarrangement = Column(Boolean,nullable=False) #5 Y'
    
    account_id = Column(Integer, ForeignKey('PCC_Account.id') ,nullable=False)
    # account = relationship(
    #     "PCC_Account",
    #     backref=backref("PCC_Arrangement",lazy=True),
    #     primaryjoin = "and_(PCC_Account.id==PCC_ARRANGEMENT.account_id)",              
    # )

    promise_amount = Column(Float, nullable=False,default=0) #4
    
class PCC_Exception(Base):
    __tablename__ = 'PCC_Exception'
    id = Column(Integer, primary_key=True, autoincrement=False)
    vip_flag = Column(Boolean, nullable=False) #9 'Y'

    account_id = Column(Integer, ForeignKey('PCC_Account.id'), nullable=False)
    # account = relationship(
    #     "PCC_Account",
    #     backref=backref("PCC_Exception",lazy=True),
    #     primaryjoin = "and_(PCC_Account.id==PCC_EXCEPTION.account_id)",
    # )
    

class PCC_Subscription(Base):
    __tablename__ = 'PCC_Subscription'
    id = Column(Integer, primary_key=True, autoincrement=False)
    subscription_status  = Column(String(50), nullable=False) #10-16 Barred,Terminated,Active

    account_id = Column(Integer, ForeignKey('PCC_Account.id'), nullable=False)
    # account = relationship(
    #     "PCC_Account",
    #     backref=backref("PCC_Subscription",lazy=True),
    #     primaryjoin = "and_(PCC_Account.id==PCC_Subscription.account_id)",
    # )
    
class PCC_Invoices(Base):
    __tablename__ = 'PCC_Invoices'
    id = Column(Integer, primary_key=True, autoincrement=False)
    invoice_status = Column(String(50),nullable=False) #21 O
    invoices_closing_balance = Column(Float, nullable=False,default=0) #21 -
    
    account_id = Column(Integer, ForeignKey('PCC_Account.id'), nullable=False)
    # account = relationship(
    #     "PCC_Account",
    #     backref=backref("PCC_Invoices",lazy=True),
    #     primaryjoin = "and_(PCC_Account.id==PCC_Invoices.account_id)",
    # )


# Base.metadata.create_all(db)    