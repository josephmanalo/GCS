from sqlalchemy import Column, Float, String, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()



class PCC_GBUAGG_DATA(Base):
    __tablename__ = "PCC_GBUAGG_DATA"
    id = Column(Integer, primary_key=True, autoincrement=True)
    TOT_ACC_FORBAR = Column(Integer)
    TOT_ACC_FORTERM = Column(Integer)
    CNT_ACTIVE_ACC = Column(Integer)
    CNT_DQ_ACC = Column(Integer)
    SUM_PROMISEAMT_GBU = Column(Float)
    CNT_BARRED_ACC = Column(Integer)
    CNT_HCA_ACC = Column(Integer)
    CNT_ADA_ARR = Column(Integer)
    CNT_VIP = Column(Integer)
    CNT_ACTIVESUBSC_CC = Column(Integer)
    CNT_BARREDSUBSC_CC = Column(Integer)
    CNT_SUBSCFORTERM_CC = Column(Integer)
    CNT_TERMSUBSC_CC = Column(Integer)
    ACCT_DQRATE = Column(Float)
    TOT_AR_DQRATE = Column(Float)
    ACTIVE_AR_DQRATE = Column(Float)
    TOT_BAL_DQ_ACC = Column(Integer)

    GBU_ID = Column(String(50))
    BUCKET1_AMOUNT = Column(Integer)
    BUCKET2_AMOUNT = Column(Integer)
    BUCKET3_AMOUNT = Column(Integer)
    BUCKET4_AMOUNT = Column(Integer)
    BUCKET5_AMOUNT = Column(Integer)
    BUCKET6_AMOUNT = Column(Integer)
    BUCKET7_AMOUNT = Column(Integer)
    BUCKET8_AMOUNT = Column(Integer)
    BUCKET9_AMOUNT = Column(Integer)
    BUCKET10_AMOUNT = Column(Integer)


class PCC_COMPGAGG_DATA(Base):
    __tablename__ = "PCC_COMPGAGG_DATA"
    id = Column(Integer, primary_key=True, autoincrement=True)
    TOT_ACC_FORBAR = Column(Integer)
    TOT_ACC_FORTERM = Column(Integer)
    CNT_ACTIVE_ACC = Column(Integer)
    CNT_DQ_ACC = Column(Integer)
    SUM_PROMISEAMT_GBU = Column(Float)
    CNT_BARRED_ACC = Column(Integer)
    CNT_HCA_ACC = Column(Integer)
    CNT_ADA_ARR = Column(Integer)
    CNT_VIP = Column(Integer)
    CNT_ACTIVESUBSC_CC = Column(Integer)
    CNT_BARREDSUBSC_CC = Column(Integer)
    CNT_SUBSCFORTERM_CC = Column(Integer)
    CNT_TERMSUBSC_CC = Column(Integer)
    ACCT_DQRATE = Column(Float)
    TOT_AR_DQRATE = Column(Float)
    ACTIVE_AR_DQRATE = Column(Float)
    TOT_BAL_DQ_ACC = Column(Integer)

    Group_ID = Column(String(50))
    BUCKET1_AMOUNT = Column(Integer)
    BUCKET2_AMOUNT = Column(Integer)
    BUCKET3_AMOUNT = Column(Integer)
    BUCKET4_AMOUNT = Column(Integer)
    BUCKET5_AMOUNT = Column(Integer)
    BUCKET6_AMOUNT = Column(Integer)
    BUCKET7_AMOUNT = Column(Integer)
    BUCKET8_AMOUNT = Column(Integer)
    BUCKET9_AMOUNT = Column(Integer)
    BUCKET10_AMOUNT = Column(Integer)


class PCC_ENTITYAGG_DATA(Base):
    __tablename__ = "PCC_ENTITYAGG_DATA"
    id = Column(Integer, primary_key=True, autoincrement=True)
    TOT_ACC_FORBAR = Column(Integer)
    TOT_ACC_FORTERM = Column(Integer)
    CNT_ACTIVE_ACC = Column(Integer)
    CNT_DQ_ACC = Column(Integer)
    SUM_PROMISEAMT_GBU = Column(Float)
    CNT_BARRED_ACC = Column(Integer)
    CNT_HCA_ACC = Column(Integer)
    CNT_ADA_ARR = Column(Integer)
    CNT_VIP = Column(Integer)
    CNT_ACTIVESUBSC_CC = Column(Integer)
    CNT_BARREDSUBSC_CC = Column(Integer)
    CNT_SUBSCFORTERM_CC = Column(Integer)
    CNT_TERMSUBSC_CC = Column(Integer)
    CNT_ACTIVE_SUBSC_IC = Column(Integer)
    CNT_BARREDSUBSC_IC = Column(Integer)
    CNT_TERMSUBSC_IC = Column(Integer)
    ACCT_DQRATE = Column(Float)
    TOT_AR_DQRATE = Column(Float)
    ACTIVE_AR_DQRATE = Column(Float)
    TOT_BAL_DQ_ACC = Column(Integer)

    Entity_ID = Column(String(50))
    BUCKET1_AMOUNT = Column(Integer)
    BUCKET2_AMOUNT = Column(Integer)
    BUCKET3_AMOUNT = Column(Integer)
    BUCKET4_AMOUNT = Column(Integer)
    BUCKET5_AMOUNT = Column(Integer)
    BUCKET6_AMOUNT = Column(Integer)
    BUCKET7_AMOUNT = Column(Integer)
    BUCKET8_AMOUNT = Column(Integer)
    BUCKET9_AMOUNT = Column(Integer)
    BUCKET10_AMOUNT = Column(Integer)


class PCC_CORPAGG_DATA(Base):
    __tablename__ = "PCC_CORPAGG_DATA"
    id = Column(Integer, primary_key=True, autoincrement=True)
    TOT_ACC_FORBAR = Column(Integer)
    TOT_ACC_FORTERM = Column(Integer)
    CNT_ACTIVE_ACC = Column(Integer)
    CNT_DQ_ACC = Column(Integer)
    SUM_PROMISEAMT_GBU = Column(Float)
    CNT_BARRED_ACC = Column(Integer)
    CNT_HCA_ACC = Column(Integer)
    CNT_ADA_ARR = Column(Integer)
    CNT_VIP = Column(Integer)
    CNT_ACTIVESUBSC_CC = Column(Integer)
    CNT_BARREDSUBSC_CC = Column(Integer)
    CNT_SUBSCFORTERM_CC = Column(Integer)
    CNT_TERMSUBSC_CC = Column(Integer)
    CNT_ACTIVE_SUBSC_IC = Column(Integer)
    CNT_BARREDSUBSC_IC = Column(Integer)
    CNT_TERMSUBSC_IC = Column(Integer)
    ACCT_DQRATE = Column(Float)
    TOT_AR_DQRATE = Column(Float)
    ACTIVE_AR_DQRATE = Column(Float)
    TOT_BAL_DQ_ACC = Column(Integer)

    Corp_ID = Column(String(50))
    BUCKET1_AMOUNT = Column(Integer)
    BUCKET2_AMOUNT = Column(Integer)
    BUCKET3_AMOUNT = Column(Integer)
    BUCKET4_AMOUNT = Column(Integer)
    BUCKET5_AMOUNT = Column(Integer)
    BUCKET6_AMOUNT = Column(Integer)
    BUCKET7_AMOUNT = Column(Integer)
    BUCKET8_AMOUNT = Column(Integer)
    BUCKET9_AMOUNT = Column(Integer)
    BUCKET10_AMOUNT = Column(Integer)


class PCC_CUSTAGG_DATA(Base):
    __tablename__ = "PCC_CUSTAGG_DATA"
    id = Column(Integer, primary_key=True, autoincrement=True)
    TOT_ACC_FORBAR = Column(Integer)
    TOT_ACC_FORTERM = Column(Integer)
    CNT_ACTIVE_ACC = Column(Integer)
    CNT_DQ_ACC = Column(Integer)
    SUM_PROMISEAMT_GBU = Column(Float)
    CNT_BARRED_ACC = Column(Integer)
    CNT_HCA_ACC = Column(Integer)
    CNT_ADA_ARR = Column(Integer)
    CNT_VIP = Column(Integer)
    CNT_ACTIVESUBSC_CC = Column(Integer)
    CNT_BARREDSUBSC_CC = Column(Integer)
    CNT_SUBSCFORTERM_CC = Column(Integer)
    CNT_TERMSUBSC_CC = Column(Integer)
    CNT_ACTIVE_SUBSC_IC = Column(Integer)
    CNT_BARREDSUBSC_IC = Column(Integer)
    CNT_TERMSUBSC_IC = Column(Integer)
    ACCT_DQRATE = Column(Float)
    TOT_AR_DQRATE = Column(Float)
    ACTIVE_AR_DQRATE = Column(Float)
    TOT_BAL_DQ_ACC = Column(Integer)

    Customer_ID = Column(String(50))
    BUCKET1_AMOUNT = Column(Integer)
    BUCKET2_AMOUNT = Column(Integer)
    BUCKET3_AMOUNT = Column(Integer)
    BUCKET4_AMOUNT = Column(Integer)
    BUCKET5_AMOUNT = Column(Integer)
    BUCKET6_AMOUNT = Column(Integer)
    BUCKET7_AMOUNT = Column(Integer)
    BUCKET8_AMOUNT = Column(Integer)
    BUCKET9_AMOUNT = Column(Integer)
    BUCKET10_AMOUNT = Column(Integer)


class PCC_Arrangement(Base):
    __tablename__ = "PCC_Arrangement"
    id = Column(Integer, primary_key=True, autoincrement=True)
    isonarrangement = Column(Boolean, nullable=False)  # 5 Y'
    promise_amount = Column(Float, nullable=False, default=0)  # 4

    account_id = Column(Integer, ForeignKey("PCC_Account.id"), nullable=False)

    GBU_ID = Column(String(50), nullable=False)
    Group_ID = Column(String(50), nullable=False)
    Entity_ID = Column(String(50), nullable=False)
    Corp_ID = Column(String(50), nullable=False)
    Customer_ID = Column(String(50), nullable=False)

class PCC_Exception(Base):
    __tablename__ = "PCC_Exception"
    id = Column(Integer, primary_key=True, autoincrement=True)
    vip_flag = Column(Boolean, nullable=False)  # 9 'Y'

    account_id = Column(Integer, ForeignKey("PCC_Account.id"), nullable=False)

    GBU_ID = Column(String(50), nullable=False)
    Group_ID = Column(String(50), nullable=False)
    Entity_ID = Column(String(50), nullable=False)
    Corp_ID = Column(String(50), nullable=False)
    Customer_ID = Column(String(50), nullable=False)

class PCC_Subscription(Base):
    __tablename__ = "PCC_Subscription"
    id = Column(Integer, primary_key=True, autoincrement=True)
    subscription_status = Column(
        String(50), nullable=False
    )  # 10-16 Barred,Terminated,Active

    account_id = Column(Integer, ForeignKey("PCC_Account.id"), nullable=False)

    GBU_ID = Column(String(50), nullable=False)
    Group_ID = Column(String(50), nullable=False)
    Entity_ID = Column(String(50), nullable=False)
    Corp_ID = Column(String(50), nullable=False)
    Customer_ID = Column(String(50), nullable=False)

class PCC_Invoices(Base):
    __tablename__ = "PCC_Invoices"
    id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_status = Column(String(50), nullable=False)  # 21 O
    invoices_closing_balance = Column(Float, nullable=False, default=0)  # 21 -

    account_id = Column(Integer, ForeignKey("PCC_Account.id"), nullable=False)

    GBU_ID = Column(String(50), nullable=False)
    Group_ID = Column(String(50), nullable=False)
    Entity_ID = Column(String(50), nullable=False)
    Corp_ID = Column(String(50), nullable=False)
    Customer_ID = Column(String(50), nullable=False)

class PCC_Account(Base):
    __tablename__ = "PCC_Account"
    id = Column(Integer, primary_key=True, autoincrement=True)
    collection_status = Column(
        String(50), nullable=False
    )  # 1-2,6,12 Barring Requested,Termination Requested \
    # For Termination, Barred,Termination
    account_status = Column(String(50), nullable=False)  # 3,19 Active
    delinquent_account = Column(Boolean, nullable=False)  # 4,17,20 'Y'
    on_hold_flag = Column(String(50), nullable=False)  # 7
    pay_means = Column(String(50), nullable=False)  # 8 Auto Debit
    outstanding_balance = Column(Float, nullable=False, default=0)  # 18,20

    GBU = Column(String(50), nullable=False)
    Custom_Group = Column(String(50), nullable=False)
    Legal_Entity = Column(String(50), nullable=False)

    GBU_ID = Column(String(50), nullable=False)
    Group_ID = Column(String(50), nullable=False)
    Entity_ID = Column(String(50), nullable=False)
    Corp_ID = Column(String(50), nullable=False)
    Customer_ID = Column(String(50), nullable=False)

    pcc_arrangement = relationship("PCC_Arrangement", backref="PCC_Account")
    pcc_exception = relationship("PCC_Exception", backref="PCC_Account")
    pcc_subscription = relationship("PCC_Subscription", backref="PCC_Account")
    pcc_invoices = relationship("PCC_Invoices", backref="PCC_Account")

# from sqlalchemy import create_engine, MetaData
# db = create_engine("postgresql+psycopg2://postgres:common@localhost:5432/postgres")
# connection = db.connect()
# metadata = MetaData(db)
# Base.metadata.create_all(db)
