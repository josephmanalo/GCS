from models import *
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker


def Count_VIP(session):
    """
    count of accounts where PCC_EXCEPTION.VIP_FLAG = 'Y', 
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.
    """

    #PCC_Exception.GBU
    gbu_data = (
        session.query(
            PCC_Exception.GBU_ID,
            func.count(PCC_Exception.vip_flag).label("Count")
        )
        .filter(PCC_Exception.vip_flag == 'Y')
        .group_by(PCC_Exception.GBU_ID, PCC_Exception.vip_flag)
    )

    #PCC_Exception.Custom_Group
    grp_data = (
        session.query(
            PCC_Exception.Group_ID,
            func.count(PCC_Exception.vip_flag).label("Count")
        )
        .filter(PCC_Exception.vip_flag == 'Y')
        .group_by(PCC_Exception.Group_ID, PCC_Exception.vip_flag)
    )

    #PCC_Exception.Legal_Entitiy
    leg_data = (
        session.query(
            PCC_Exception.Entity_ID,
            func.count(PCC_Exception.vip_flag).label("Count")
        )
        .filter(PCC_Exception.vip_flag == 'Y')
        .group_by(PCC_Exception.Entity_ID, PCC_Exception.vip_flag)
    )

    #PCC_Exception.Corp_ID
    crp_data = (
        session.query(
            PCC_Exception.Corp_ID,
            func.count(PCC_Exception.vip_flag).label("Count")
        )
        .filter(PCC_Exception.vip_flag == 'Y')
        .group_by(PCC_Exception.Corp_ID, PCC_Exception.vip_flag)
    )

    #PCC_Exception.Customer_Id
    cst_data = (
        session.query(
            PCC_Exception.Customer_ID,
            func.count(PCC_Exception.vip_flag).label("Count")
        )
        .filter(PCC_Exception.vip_flag == 'Y')
        .group_by(PCC_Exception.Customer_ID, PCC_Exception.vip_flag)
    )


    # Insert into DB 
    for i in gbu_data:
        session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, CNT_VIP=i.Count))

    for i in grp_data:
        session.add(PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, CNT_VIP=i.Count))

    for i in leg_data:
        session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, CNT_VIP=i.Count))

    for i in crp_data:
        session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, CNT_VIP=i.Count))

    for i in cst_data:
        session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, CNT_VIP=i.Count))

    session.commit()






