from sqlalchemy.orm import Session
from models import TopcvDataJob, ItviecDataJob
from schemas import TopcvDataJob as TopcvDataJobSchema, ItviecDataJob as ItviecDataJobSchema
from typing import List

def get_topcv_jobs(db: Session) -> List[TopcvDataJobSchema]:
    """
    Retrieve all TopCV jobs from the database
    """
    return db.query(TopcvDataJob).all()

def get_itviec_jobs(db: Session) -> List[ItviecDataJobSchema]:
    """
    Retrieve all ITViec jobs from the database
    """
    return db.query(ItviecDataJob).all()