from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    Numeric,
    String,
)
from sqlalchemy.orm import relationship

from .trial_metadata import Sample
from .file_metadata import Upload, HandeImage


class HandeUpload(Upload):
    __mapper_args__ = {"polymorphic_identity": "hande"}

    records = relationship(
        "HandeRecord", back_populates="upload", sync_backref=False, viewonly=True
    )
    images = relationship(
        HandeImage, back_populates="upload", sync_backref=False, viewonly=True
    )


class HandeRecord(HandeUpload):
    __tablename__ = "hande_records"
    assay_id = Column(Integer, nullable=False)
    trial_id = Column(String, nullable=False)
    cimac_id = Column(String, nullable=False)
    image_url = Column(String, ForeignKey(HandeImage.object_url), nullable=False)

    tumor_tissue_percentage = Column(
        Numeric,
        CheckConstraint(
            "tumor_tissue_percentage >= 0 and tumor_tissue_percentage <= 100"
        ),
    )
    viable_tumor_percentage = Column(
        Numeric,
        CheckConstraint(
            "viable_tumor_percentage >= 0 and viable_tumor_percentage <= 100"
        ),
    )
    viable_stroma_percentage = Column(
        Numeric,
        CheckConstraint(
            "viable_stroma_percentage >= 0 and viable_stroma_percentage <= 100"
        ),
    )
    necrosis_percentage = Column(
        Numeric,
        CheckConstraint("necrosis_percentage >= 0 and necrosis_percentage <= 100"),
    )
    fibrosis_percentage = Column(
        Numeric,
        CheckConstraint("fibrosis_percentage >= 0 and fibrosis_percentage <= 100"),
    )
    comment = Column(String)

    image = relationship(
        HandeImage, back_populates="record", sync_backref=False, viewonly=True
    )
    upload = relationship(
        HandeUpload, back_populates="records", sync_backref=False, viewonly=True
    )

    __table_args__ = (
        ForeignKeyConstraint(
            [trial_id, assay_id], [HandeUpload.trial_id, HandeUpload.id]
        ),
        ForeignKeyConstraint([trial_id, cimac_id], [Sample.trial_id, Sample.cimac_id]),
    )
