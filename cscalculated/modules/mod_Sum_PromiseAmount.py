from models import *
from sqlalchemy.sql import func

def Sum_PromiseAmount(session):
    """
    SUM(PCC_ARRANGEMENT.PROMISE_AMOUNT) WHERE ISONARRANGEMENT = 'Y'
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.
    """

    gbu_data = (
        session.query(
            PCC_Arrangement.GBU_ID,
            func.sum(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
        )
        .where(PCC_Arrangement.isonarrangement == 'Y')
        .group_by(PCC_Arrangement.GBU_ID, PCC_Arrangement.isonarrangement)
    )
    

    grp_data = (
        session.query(
            PCC_Arrangement.Group_ID,
            func.sum(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
        )
        .where(PCC_Arrangement.isonarrangement == 'Y')
        .group_by(PCC_Arrangement.Group_ID, PCC_Arrangement.isonarrangement)
    )


    leg_data = (
        session.query(
            PCC_Arrangement.Entity_ID,
            func.sum(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
        )
        .where(PCC_Arrangement.isonarrangement == 'Y')
        .group_by(PCC_Arrangement.Entity_ID, PCC_Arrangement.isonarrangement)
    )


    crp_data = (
        session.query(
            PCC_Arrangement.Corp_ID,
            func.sum(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
        )
        .where(PCC_Arrangement.isonarrangement == 'Y')
        .group_by(PCC_Arrangement.Corp_ID, PCC_Arrangement.isonarrangement)
    )


    cst_data = (
        session.query(
            PCC_Arrangement.Customer_ID,
            func.sum(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
        )
        .where(PCC_Arrangement.isonarrangement == 'Y')
        .group_by(PCC_Arrangement.Customer_ID, PCC_Arrangement.isonarrangement)
    )



    # Insert into DB 
    for i in gbu_data:
        session.add(
            PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, SUM_PROMISEAMT_GBU=i.sum_promise_amount)
        )

    for i in grp_data:
        session.add(
            PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, SUM_PROMISEAMT_GBU=i.sum_promise_amount)
        )

    for i in leg_data:
        session.add(
            PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, SUM_PROMISEAMT_GBU=i.sum_promise_amount)
        )

    for i in crp_data:
        session.add(
            PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, SUM_PROMISEAMT_GBU=i.sum_promise_amount)
        )
        
    for i in cst_data:
        session.add(
            PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, SUM_PROMISEAMT_GBU=i.sum_promise_amount)
        )

    session.commit()





