from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    Integer,
    Numeric,
    String,
)
from sqlalchemy.orm import relationship

from .model_core import MetadataModel
from .trial_metadata import Sample
from .file_metadata import Upload, ImageFile


class HandeUpload(Upload):
    records = relationship(
        "HandeRecord", back_populates="upload", sync_backref=False, viewonly=True
    )
    images = relationship(
        "HandeImage", back_populates="upload", sync_backref=False, viewonly=True
    )

    __mapper_args__ = {"polymorphic_identity": "hande"}


class HandeImage(ImageFile):
    __tablename__ = "hande_images"
    __mapper_args__ = {"polymorphic_identity": "hande_image.svs"}

    object_url = Column(String, primary_key=True)
    upload_id = Column(Integer, nullable=False)

    upload = relationship(
        HandeUpload, back_populates="images", sync_backref=False, viewonly=True
    )

    __table_args__ = (
        ForeignKeyConstraint(
            [upload_id, object_url], [ImageFile.upload_id, ImageFile.object_url]
        ),
    )


class HandeRecord(MetadataModel):
    __tablename__ = "hande_records"
    assay_id = Column(Integer, nullable=False, primary_key=True)
    cimac_id = Column(String, nullable=False, primary_key=True)
    trial_id = Column(String, nullable=False)
    image_url = Column(String, nullable=False)

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

    upload = relationship(
        HandeUpload, back_populates="records", sync_backref=False, viewonly=True
    )

    __table_args__ = (
        ForeignKeyConstraint(
            [trial_id, assay_id], [HandeUpload.trial_id, HandeUpload.id]
        ),
        ForeignKeyConstraint([trial_id, cimac_id], [Sample.trial_id, Sample.cimac_id]),
    )

    @property
    def image(self) -> HandeImage:
        return HandeImage.get_by_id(self.image_url, self.assay_id)
