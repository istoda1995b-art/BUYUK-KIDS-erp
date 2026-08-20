"""
Buyuk Kids — ERP сервер

1С ҳар 5 дақиқада савдо маълумотларини юборади, сайт шу ердан ўқийди.

  GET  /v1/sync/holat            → 1С сўрайди: охиргиси қачон эди?
  POST /v1/sync/cheklar          → 1С юборади: чеклар + сатрлар
  GET  /v1/hisobot/kunlik        → кунлар бўйича савдо ва фойда
  GET  /v1/hisobot/top           → товар / сотувчи / мижоз кесимида
  GET  /v1/cheklar               → бир кундаги чеклар рўйхати
  GET  /v1/chek/{uid}            → чек таркиби

Аутентификация: HTTP Basic (1С ҳам, сайт ҳам).
"""

import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey, Index, Numeric, String,
    delete, func, select, text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("erp")

# ─────────────────────────── Созламалар ───────────────────────────


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"{name} муҳит ўзгарувчиси созланмаган")
    return v


def _async_dsn(raw: str) -> str:
    if raw.startswith("postgresql+"):
        return raw
    if raw.startswith("postgresql://"):
        return raw.replace("postgresql://", "postgresql+asyncpg://", 1)
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql+asyncpg://", 1)
    return raw


DATABASE_URL = _async_dsn(_env("DATABASE_URL"))
ERP_USERNAME = _env("ERP_USERNAME")
ERP_PASSWORD = _env("ERP_PASSWORD")
# 1С биринчи марта уланганда шунча кун орқага қайтиб юборади
BOSHLANGICH_KUN = int(os.getenv("BOSHLANGICH_KUN", "30"))

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5, max_overflow=5)
Session = async_sessionmaker(engine, expire_on_commit=False)

# ─────────────────────────── Моделлар ───────────────────────────


class Base(DeclarativeBase):
    pass


class Sale(Base):
    """Чек — КассирОйнасиХужжатлариБезОстатка."""

    __tablename__ = "sales"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    hujjat_uid: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    raqam: Mapped[str] = mapped_column(String(30), default="")
    sana: Mapped[datetime] = mapped_column(DateTime(timezone=False), index=True)

    savdo_nuqtasi: Mapped[str] = mapped_column(String(150), default="")
    sotuvchi: Mapped[str] = mapped_column(String(150), default="", index=True)
    mijoz: Mapped[str] = mapped_column(String(200), default="", index=True)
    karta_raqami: Mapped[str] = mapped_column(String(30), default="")

    jami_summa: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    chegirma: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    tulov_uchun: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)

    naqd: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    uzcard: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    humo: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    click: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    payme: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    uzum: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    bank: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    ball: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)

    tannarx: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    foyda: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)

    qaytarish: Mapped[bool] = mapped_column(Boolean, default=False)
    onlayn_chek: Mapped[bool] = mapped_column(Boolean, default=False)

    yangilandi: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class SaleLine(Base):
    """Чек сатри — СотилганТоварларРегистри ҳаракатидан."""

    __tablename__ = "sale_lines"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    hujjat_uid: Mapped[str] = mapped_column(String(50), index=True)
    sana: Mapped[datetime] = mapped_column(DateTime(timezone=False), index=True)

    tovar: Mapped[str] = mapped_column(String(300), default="", index=True)
    tovar_kodi: Mapped[str] = mapped_column(String(30), default="")
    guruh: Mapped[str] = mapped_column(String(200), default="")
    sotuvchi: Mapped[str] = mapped_column(String(150), default="")
    mijoz: Mapped[str] = mapped_column(String(200), default="")

    soni: Mapped[Decimal] = mapped_column(Numeric(15, 3), default=0)
    summa: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    tannarx: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    foyda: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)


Index("ix_lines_sana_tovar", SaleLine.sana, SaleLine.tovar)


class SyncState(Base):
    """1С қаергача юборганини сервер эслаб қолади."""

    __tablename__ = "sync_state"

    kalit: Mapped[str] = mapped_column(String(50), primary_key=True)
    qiymat: Mapped[str] = mapped_column(String(50), default="")
    yangilandi: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


# ─────────────────────────── Хавфсизлик ───────────────────────────

basic = HTTPBasic(auto_error=True)


def check_auth(c: HTTPBasicCredentials = Depends(basic)) -> str:
    ok_u = secrets.compare_digest(c.username, ERP_USERNAME)
    ok_p = secrets.compare_digest(c.password, ERP_PASSWORD)
    if not (ok_u and ok_p):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Логин ёки парол нотўғри",
            headers={"WWW-Authenticate": "Basic"},
        )
    return c.username


async def get_session() -> AsyncSession:
    async with Session() as s:
        yield s


guard = [Depends(check_auth)]

# ─────────────────────────── Схемалар ───────────────────────────


class ChekIn(BaseModel):
    hujjat_uid: str = Field(max_length=50)
    raqam: str = Field(default="", max_length=30)
    sana: str  # "yyyyMMddHHmmss"
    savdo_nuqtasi: str = Field(default="", max_length=150)
    sotuvchi: str = Field(default="", max_length=150)
    mijoz: str = Field(default="", max_length=200)
    karta_raqami: str = Field(default="", max_length=30)
    jami_summa: float = 0
    chegirma: float = 0
    tulov_uchun: float = 0
    naqd: float = 0
    uzcard: float = 0
    humo: float = 0
    click: float = 0
    payme: float = 0
    uzum: float = 0
    bank: float = 0
    ball: float = 0
    qaytarish: bool = False
    onlayn_chek: bool = False


class SatrIn(BaseModel):
    hujjat_uid: str = Field(max_length=50)
    sana: str
    tovar: str = Field(default="", max_length=300)
    tovar_kodi: str = Field(default="", max_length=30)
    guruh: str = Field(default="", max_length=200)
    sotuvchi: str = Field(default="", max_length=150)
    mijoz: str = Field(default="", max_length=200)
    soni: float = 0
    summa: float = 0
    tannarx: float = 0
    foyda: float = 0


class SyncIn(BaseModel):
    cheklar: list[ChekIn] = []
    satrlar: list[SatrIn] = []
    gacha: str  # 1С шу вақтгача юборди


# ─────────────────────────── Ёрдамчилар ───────────────────────────


def sana_oqi(matn: str) -> datetime:
    """1С форматидан: yyyyMMddHHmmss ёки yyyyMMdd."""
    matn = (matn or "").strip()
    try:
        if len(matn) >= 14:
            return datetime.strptime(matn[:14], "%Y%m%d%H%M%S")
        if len(matn) == 8:
            return datetime.strptime(matn, "%Y%m%d")
    except ValueError:
        pass
    raise HTTPException(status_code=400, detail=f"Сана нотўғри: {matn}")


def sana_matn(d: datetime) -> str:
    return d.strftime("%Y%m%d%H%M%S")


def kun_oqi(matn: str | None, standart: datetime) -> datetime:
    matn = (matn or "").strip()
    if len(matn) == 8:
        try:
            return datetime.strptime(matn, "%Y%m%d")
        except ValueError:
            pass
    return standart


def pul(v: Any) -> float:
    return round(float(v or 0), 2)


# ─────────────────────────── Илова ───────────────────────────

app = FastAPI(title="Buyuk Kids ERP", docs_url=None, redoc_url=None)


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"success": False, "message": exc.detail, "data": {}},
        headers=exc.headers or {},
    )


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception) -> JSONResponse:
    log.exception("Кутилмаган хато: %s", request.url.path)
    return JSONResponse(
        status_code=500,
        content={"success": False, "message": "Ички хатолик", "data": {}},
    )


@app.on_event("startup")
async def startup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("ERP сервер тайёр")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


def javob(data: Any, message: str = "") -> dict:
    return {"success": True, "message": message, "data": data}


# ── 1С: қаергача юборганман? ──────────────────────────


@app.get("/v1/sync/holat", dependencies=guard)
async def sync_holat(session: AsyncSession = Depends(get_session)) -> dict:
    row = await session.get(SyncState, "oxirgi_sana")
    if row and row.qiymat:
        oxirgi = row.qiymat
    else:
        oxirgi = sana_matn(datetime.now() - timedelta(days=BOSHLANGICH_KUN))

    jami = await session.scalar(select(func.count(Sale.id))) or 0
    return javob({"oxirgi_sana": oxirgi, "cheklar_soni": jami})


# ── 1С: чекларни юбориш ───────────────────────────────


@app.post("/v1/sync/cheklar", dependencies=guard)
async def sync_cheklar(
    payload: SyncIn, session: AsyncSession = Depends(get_session)
) -> dict:
    # Чекларни upsert қиламиз — қайта юборилса ҳам дубликат бўлмайди
    for c in payload.cheklar:
        qiymatlar = {
            "hujjat_uid": c.hujjat_uid,
            "raqam": c.raqam,
            "sana": sana_oqi(c.sana),
            "savdo_nuqtasi": c.savdo_nuqtasi,
            "sotuvchi": c.sotuvchi,
            "mijoz": c.mijoz,
            "karta_raqami": c.karta_raqami,
            "jami_summa": pul(c.jami_summa),
            "chegirma": pul(c.chegirma),
            "tulov_uchun": pul(c.tulov_uchun),
            "naqd": pul(c.naqd),
            "uzcard": pul(c.uzcard),
            "humo": pul(c.humo),
            "click": pul(c.click),
            "payme": pul(c.payme),
            "uzum": pul(c.uzum),
            "bank": pul(c.bank),
            "ball": pul(c.ball),
            "qaytarish": c.qaytarish,
            "onlayn_chek": c.onlayn_chek,
        }
        stmt = pg_insert(Sale).values(**qiymatlar)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Sale.hujjat_uid],
            set_={k: v for k, v in qiymatlar.items() if k != "hujjat_uid"},
        )
        await session.execute(stmt)

    # Сатрлар: ҳужжат бўйича тозалаб, қайта ёзамиз
    uidlar = {s.hujjat_uid for s in payload.satrlar}
    if uidlar:
        await session.execute(
            delete(SaleLine).where(SaleLine.hujjat_uid.in_(uidlar))
        )

    for s in payload.satrlar:
        session.add(
            SaleLine(
                hujjat_uid=s.hujjat_uid,
                sana=sana_oqi(s.sana),
                tovar=s.tovar,
                tovar_kodi=s.tovar_kodi,
                guruh=s.guruh,
                sotuvchi=s.sotuvchi,
                mijoz=s.mijoz,
                soni=round(float(s.soni or 0), 3),
                summa=pul(s.summa),
                tannarx=pul(s.tannarx),
                foyda=pul(s.foyda),
            )
        )

    # Чекнинг таннарх/фойдасини сатрлардан йиғамиз
    if uidlar:
        await session.flush()
        await session.execute(
            text(
                """
                UPDATE sales s SET
                    tannarx = COALESCE(a.tannarx, 0),
                    foyda   = COALESCE(a.foyda, 0)
                FROM (
                    SELECT hujjat_uid,
                           SUM(tannarx) AS tannarx,
                           SUM(foyda)   AS foyda
                    FROM sale_lines
                    WHERE hujjat_uid = ANY(:uidlar)
                    GROUP BY hujjat_uid
                ) a
                WHERE s.hujjat_uid = a.hujjat_uid
                """
            ),
            {"uidlar": list(uidlar)},
        )

    # Маркерни силжитамиз
    holat = await session.get(SyncState, "oxirgi_sana")
    if holat is None:
        session.add(SyncState(kalit="oxirgi_sana", qiymat=payload.gacha))
    else:
        holat.qiymat = payload.gacha

    await session.commit()

    log.info(
        "Синхронизация: %s чек, %s сатр, %s гача",
        len(payload.cheklar), len(payload.satrlar), payload.gacha,
    )
    return javob(
        {
            "cheklar": len(payload.cheklar),
            "satrlar": len(payload.satrlar),
            "oxirgi_sana": payload.gacha,
        }
    )


# ── Сайт: кунлик ҳисобот ──────────────────────────────


@app.get("/v1/hisobot/kunlik", dependencies=guard)
async def hisobot_kunlik(
    dan: str | None = Query(None),
    gacha: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
) -> dict:
    bugun = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    d1 = kun_oqi(dan, bugun - timedelta(days=29))
    d2 = kun_oqi(gacha, bugun) + timedelta(days=1)

    rows = (
        await session.execute(
            text(
                """
                SELECT DATE(sana) AS kun,
                       COUNT(*)                AS cheklar,
                       SUM(tulov_uchun)        AS savdo,
                       SUM(tannarx)            AS tannarx,
                       SUM(foyda)              AS foyda,
                       SUM(chegirma)           AS chegirma,
                       SUM(naqd)               AS naqd,
                       SUM(uzcard + humo + click + payme + uzum + bank) AS karta,
                       SUM(ball)               AS ball
                FROM sales
                WHERE sana >= :d1 AND sana < :d2 AND NOT qaytarish
                GROUP BY DATE(sana)
                ORDER BY kun DESC
                """
            ),
            {"d1": d1, "d2": d2},
        )
    ).all()

    kunlar = []
    for r in rows:
        savdo = float(r.savdo or 0)
        foyda = float(r.foyda or 0)
        kunlar.append(
            {
                "kun": r.kun.strftime("%Y-%m-%d"),
                "cheklar": int(r.cheklar or 0),
                "savdo": round(savdo),
                "tannarx": round(float(r.tannarx or 0)),
                "foyda": round(foyda),
                "chegirma": round(float(r.chegirma or 0)),
                "naqd": round(float(r.naqd or 0)),
                "karta": round(float(r.karta or 0)),
                "ball": round(float(r.ball or 0)),
                "marja_foizi": round(foyda / savdo * 100, 1) if savdo else 0,
                "urtacha_chek": round(savdo / r.cheklar) if r.cheklar else 0,
            }
        )

    jami_savdo = sum(k["savdo"] for k in kunlar)
    jami_foyda = sum(k["foyda"] for k in kunlar)
    jami_chek = sum(k["cheklar"] for k in kunlar)

    return javob(
        {
            "dan": d1.strftime("%Y-%m-%d"),
            "gacha": (d2 - timedelta(days=1)).strftime("%Y-%m-%d"),
            "jami": {
                "savdo": jami_savdo,
                "tannarx": sum(k["tannarx"] for k in kunlar),
                "foyda": jami_foyda,
                "cheklar": jami_chek,
                "marja_foizi": round(jami_foyda / jami_savdo * 100, 1) if jami_savdo else 0,
                "urtacha_chek": round(jami_savdo / jami_chek) if jami_chek else 0,
            },
            "kunlar": kunlar,
        }
    )


# ── Сайт: топ рўйхат ──────────────────────────────────

KESIMLAR = {"tovar": "tovar", "sotuvchi": "sotuvchi", "mijoz": "mijoz", "guruh": "guruh"}


@app.get("/v1/hisobot/top", dependencies=guard)
async def hisobot_top(
    dan: str | None = Query(None),
    gacha: str | None = Query(None),
    tur: str = Query("tovar"),
    limit: int = Query(30, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> dict:
    ustun = KESIMLAR.get(tur, "tovar")
    bugun = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    d1 = kun_oqi(dan, bugun.replace(day=1))
    d2 = kun_oqi(gacha, bugun) + timedelta(days=1)

    rows = (
        await session.execute(
            text(
                f"""
                SELECT {ustun} AS nomi,
                       SUM(soni)    AS soni,
                       SUM(summa)   AS savdo,
                       SUM(tannarx) AS tannarx,
                       SUM(foyda)   AS foyda
                FROM sale_lines
                WHERE sana >= :d1 AND sana < :d2
                GROUP BY {ustun}
                ORDER BY foyda DESC
                LIMIT :lim
                """
            ),
            {"d1": d1, "d2": d2, "lim": limit},
        )
    ).all()

    jami_savdo = float(
        await session.scalar(
            select(func.coalesce(func.sum(SaleLine.summa), 0)).where(
                SaleLine.sana >= d1, SaleLine.sana < d2
            )
        )
        or 0
    )

    royxat = []
    for r in rows:
        savdo = float(r.savdo or 0)
        foyda = float(r.foyda or 0)
        royxat.append(
            {
                "nomi": r.nomi or "—",
                "soni": round(float(r.soni or 0), 3),
                "savdo": round(savdo),
                "tannarx": round(float(r.tannarx or 0)),
                "foyda": round(foyda),
                "marja_foizi": round(foyda / savdo * 100, 1) if savdo else 0,
                "ulush_foizi": round(savdo / jami_savdo * 100, 1) if jami_savdo else 0,
            }
        )

    return javob(
        {
            "dan": d1.strftime("%Y-%m-%d"),
            "gacha": (d2 - timedelta(days=1)).strftime("%Y-%m-%d"),
            "tur": tur,
            "jami_savdo": round(jami_savdo),
            "royxat": royxat,
        }
    )


# ── Сайт: бир кундаги чеклар ──────────────────────────


@app.get("/v1/cheklar", dependencies=guard)
async def cheklar(
    kun: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    session: AsyncSession = Depends(get_session),
) -> dict:
    bugun = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    d1 = kun_oqi(kun, bugun)
    d2 = d1 + timedelta(days=1)

    rows = (
        await session.execute(
            select(Sale)
            .where(Sale.sana >= d1, Sale.sana < d2)
            .order_by(Sale.sana.desc())
            .limit(limit)
        )
    ).scalars().all()

    return javob(
        {
            "kun": d1.strftime("%Y-%m-%d"),
            "cheklar": [
                {
                    "uid": s.hujjat_uid,
                    "raqam": s.raqam,
                    "vaqt": s.sana.strftime("%H:%M"),
                    "sotuvchi": s.sotuvchi,
                    "mijoz": s.mijoz,
                    "summa": round(float(s.tulov_uchun)),
                    "foyda": round(float(s.foyda)),
                    "naqd": round(float(s.naqd)),
                    "karta": round(
                        float(s.uzcard + s.humo + s.click + s.payme + s.uzum + s.bank)
                    ),
                    "ball": round(float(s.ball)),
                    "qaytarish": s.qaytarish,
                }
                for s in rows
            ],
        }
    )


# ── Сайт: чек таркиби ─────────────────────────────────


@app.get("/v1/chek/{uid}", dependencies=guard)
async def chek(uid: str, session: AsyncSession = Depends(get_session)) -> dict:
    s = await session.scalar(select(Sale).where(Sale.hujjat_uid == uid))
    if s is None:
        raise HTTPException(status_code=404, detail="Чек топилмади")

    lines = (
        await session.execute(
            select(SaleLine).where(SaleLine.hujjat_uid == uid).order_by(SaleLine.id)
        )
    ).scalars().all()

    return javob(
        {
            "uid": s.hujjat_uid,
            "raqam": s.raqam,
            "sana": s.sana.strftime("%Y-%m-%d %H:%M"),
            "savdo_nuqtasi": s.savdo_nuqtasi,
            "sotuvchi": s.sotuvchi,
            "mijoz": s.mijoz,
            "karta_raqami": s.karta_raqami,
            "jami_summa": round(float(s.jami_summa)),
            "chegirma": round(float(s.chegirma)),
            "tulov_uchun": round(float(s.tulov_uchun)),
            "tannarx": round(float(s.tannarx)),
            "foyda": round(float(s.foyda)),
            "qaytarish": s.qaytarish,
            "tulovlar": {
                "naqd": round(float(s.naqd)),
                "uzcard": round(float(s.uzcard)),
                "humo": round(float(s.humo)),
                "click": round(float(s.click)),
                "payme": round(float(s.payme)),
                "uzum": round(float(s.uzum)),
                "bank": round(float(s.bank)),
                "ball": round(float(s.ball)),
            },
            "satrlar": [
                {
                    "tovar": l.tovar,
                    "kod": l.tovar_kodi,
                    "soni": round(float(l.soni), 3),
                    "summa": round(float(l.summa)),
                    "tannarx": round(float(l.tannarx)),
                    "foyda": round(float(l.foyda)),
                }
                for l in lines
            ],
        }
    )
