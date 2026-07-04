from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app import models, schemas
from app.auth import get_current_user
from app.database import get_db

router = APIRouter(prefix="/api", tags=["projects"])


@router.post("/organizations", response_model=schemas.OrganizationOut, status_code=201)
def create_organization(
    payload: schemas.OrganizationCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    org = models.Organization(name=payload.name, owner_id=current_user.id)
    db.add(org)
    db.commit()
    db.refresh(org)
    return org


@router.get("/organizations", response_model=list[schemas.OrganizationOut])
def list_organizations(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return db.query(models.Organization).filter(models.Organization.owner_id == current_user.id).all()


@router.post("/projects", response_model=schemas.ProjectOut, status_code=201)
def create_project(
    payload: schemas.ProjectCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    org = db.query(models.Organization).filter(
        models.Organization.id == payload.organization_id,
        models.Organization.owner_id == current_user.id,
    ).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    existing = db.query(models.Project).filter(
        models.Project.organization_id == org.id, models.Project.name == payload.name
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Project name already exists in this organization")

    project = models.Project(name=payload.name, organization_id=org.id)
    db.add(project)
    db.commit()
    db.refresh(project)
    return project


@router.get("/projects", response_model=list[schemas.ProjectOut])
def list_projects(
    organization_id: str | None = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    query = db.query(models.Project).join(models.Organization).filter(
        models.Organization.owner_id == current_user.id
    )
    if organization_id:
        query = query.filter(models.Project.organization_id == organization_id)
    return query.all()
