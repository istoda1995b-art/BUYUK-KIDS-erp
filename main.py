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
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey, Index, Numeric, String,
    delete, func, select, text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("erp")

# ─────────────────────────── Созламалар ───────────────────────────


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    # Бўш сатр ҳам "созланмаган" деб ҳисобланади — Railway'да кўп учрайди
    if v is None or not str(v).strip():
        raise RuntimeError(
            f"{name} созланмаган ёки бўш. "
            f"Railway → Variables дан текширинг."
        )
    return str(v).strip()


def _async_dsn(raw: str) -> str:
    if raw.startswith("postgresql+"):
        return raw
    if raw.startswith("postgresql://"):
        return raw.replace("postgresql://", "postgresql+asyncpg://", 1)
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql+asyncpg://", 1)
    return raw


_DSN_XOM = _env("DATABASE_URL")
if _DSN_XOM.startswith("${{") or "{{" in _DSN_XOM:
    raise RuntimeError(
        "DATABASE_URL қиймати ечилмаган: " + _DSN_XOM[:60] +
        " — Railway'да Postgres'га reference тўғри қўйилганини текширинг."
    )
DATABASE_URL = _async_dsn(_DSN_XOM)


def _foydalanuvchilar() -> dict[str, dict[str, str]]:
    """
    ERP_USERS = "admin:parol1:admin,yordamchi:parol2:yordamchi,onec:parol3:onec"

    Учинчи қисм — рол, ихтиёрий (кўрсатилмаса "admin"):
      admin      — ҳаммасига рухсат
      yordamchi  — кирим, инвентаризация, чеклар, маҳсулотлар
      onec       — фақат 1С синхронизацияси
    Парол ичида вергул ва икки нуқта бўлмасин.
    """
    xom = os.getenv("ERP_USERS", "").strip()
    natija: dict[str, dict[str, str]] = {}

    if xom:
        for juft in xom.split(","):
            juft = juft.strip()
            if not juft or ":" not in juft:
                continue
            qism = [x.strip() for x in juft.split(":")]
            nom = qism[0]
            parol = qism[1] if len(qism) > 1 else ""
            rol = qism[2] if len(qism) > 2 and qism[2] else "admin"
            if nom and parol:
                natija[nom] = {"parol": parol, "rol": rol}

    yakka_nom = os.getenv("ERP_USERNAME", "").strip()
    yakka_parol = os.getenv("ERP_PASSWORD", "").strip()
    if yakka_nom and yakka_parol:
        natija.setdefault(yakka_nom, {"parol": yakka_parol, "rol": "admin"})

    if not natija:
        raise RuntimeError(
            "ERP_USERS созланмаган. Намуна: admin:parol1:admin,yordamchi:parol2:yordamchi"
        )
    return natija


FOYDALANUVCHILAR = _foydalanuvchilar()

# Рол → рухсат этилган бўлимлар. admin учун чекланиш йўқ.
ROL_BOLIMLARI = {
    "yordamchi": ["cheklar", "mahsulot", "inv", "kirim"],
    "onec": [],
}

# Ролга берилмаган эндпоинтлар. Меню яшириш камлик қилади —
# сервер ҳам текширади, акс ҳолда манзилни қўлда ёзиб кириш мумкин.
FAQAT_ADMIN = {"hisobot_kunlik", "hisobot_top_kesim"}


def rol_ol(kim: str) -> str:
    return FOYDALANUVCHILAR.get(kim, {}).get("rol", "admin")


def admin_kerak(kim: str) -> None:
    if rol_ol(kim) != "admin":
        raise HTTPException(status_code=403, detail="Бу бўлимга рухсат йўқ")
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


class Stock(Base):
    """Омбор қолдиғи — 1С тўлиқ суратни юборади."""

    __tablename__ = "stock"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tovar_uid: Mapped[str] = mapped_column(String(50), index=True)
    ombor: Mapped[str] = mapped_column(String(150), default="", index=True)
    tovar: Mapped[str] = mapped_column(String(300), default="", index=True)
    tovar_kodi: Mapped[str] = mapped_column(String(30), default="", index=True)
    shk: Mapped[str] = mapped_column(String(30), default="", index=True)
    guruh: Mapped[str] = mapped_column(String(200), default="")
    birlik: Mapped[str] = mapped_column(String(30), default="")

    soni: Mapped[Decimal] = mapped_column(Numeric(15, 3), default=0)
    tannarx: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    sotish_narxi: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)

    yangilandi: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


Index("ix_stock_ombor_tovar", Stock.ombor, Stock.tovar_uid, unique=True)


class Taminotchi(Base):
    """Контрагентлар — 1С юборади, сайт танлаш учун ишлатади."""

    __tablename__ = "taminotchi"

    uid: Mapped[str] = mapped_column(String(50), primary_key=True)
    nomi: Mapped[str] = mapped_column(String(250), default="", index=True)
    turi: Mapped[str] = mapped_column(String(50), default="")
    telefon: Mapped[str] = mapped_column(String(50), default="")
    yangilandi: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Navbat(Base):
    """Сайтдан 1С га топшириқ — инвентаризация, кирим ва ҳ.к."""

    __tablename__ = "navbat"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    turi: Mapped[str] = mapped_column(String(30), index=True)  # inventarizatsiya
    holat: Mapped[str] = mapped_column(String(20), default="kutmoqda", index=True)
    # kutmoqda → bajarildi → xato

    ombor: Mapped[str] = mapped_column(String(150), default="")
    izoh: Mapped[str] = mapped_column(String(300), default="")
    kim: Mapped[str] = mapped_column(String(50), default="")

    tana: Mapped[dict] = mapped_column(JSONB, default=dict)
    natija: Mapped[str] = mapped_column(String(500), default="")
    hujjat_raqami: Mapped[str] = mapped_column(String(50), default="")

    yaratildi: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    bajarildi: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


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
    # Номи топилмаса ҳам вақт бир хил кетсин — ном таxмин қилинмасин
    yozuv = FOYDALANUVCHILAR.get(c.username)
    kutilgan = yozuv["parol"] if yozuv else ""
    mos = secrets.compare_digest(c.password, kutilgan) if kutilgan else False
    if not mos:
        log.warning("Кириш рад этилди: %s", c.username[:40])
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
    log.info("ERP сервер тайёр. Фойдаланувчилар: %s",
             ", ".join(f"{n} ({v['rol']})" for n, v in sorted(FOYDALANUVCHILAR.items())))


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
    kim: str = Depends(check_auth),
    session: AsyncSession = Depends(get_session),
) -> dict:
    admin_kerak(kim)
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
    kim: str = Depends(check_auth),
    session: AsyncSession = Depends(get_session),
) -> dict:
    # Ёрдамчи фақат маҳсулот кесимини кўради — сотувчи/мижоз кесими бошқарув учун
    if tur != "tovar":
        admin_kerak(kim)
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


# ── Сайт: мен кимман ──────────────────────────────────


@app.get("/v1/men", dependencies=guard)
async def men(kim: str = Depends(check_auth)) -> dict:
    yozuv = FOYDALANUVCHILAR.get(kim, {})
    rol = yozuv.get("rol", "admin")
    return javob({
        "nom": kim,
        "rol": rol,
        "bolimlar": ROL_BOLIMLARI.get(rol),  # None → чекланиш йўқ
    })


# ── 1С: қолдиқ юбориш ─────────────────────────────────


class QoldiqIn(BaseModel):
    tovar_uid: str = Field(max_length=50)
    ombor: str = Field(default="", max_length=150)
    tovar: str = Field(default="", max_length=300)
    tovar_kodi: str = Field(default="", max_length=30)
    shk: str = Field(default="", max_length=30)
    guruh: str = Field(default="", max_length=200)
    birlik: str = Field(default="", max_length=30)
    soni: float = 0
    tannarx: float = 0
    sotish_narxi: float = 0


class QoldiqSyncIn(BaseModel):
    qoldiqlar: list[QoldiqIn] = []
    # Тўлиқ сурат бўлса — эскисини тозалаймиз. Ўзгарганини юборса — йўқ.
    tozalash: bool = False
    # Қолдиғи нолга тушган (рўйхатдан чиққан) товарлар
    ochirish: list[str] = []


@app.post("/v1/sync/qoldiq", dependencies=guard)
async def sync_qoldiq(
    payload: QoldiqSyncIn, session: AsyncSession = Depends(get_session)
) -> dict:
    if payload.tozalash:
        await session.execute(delete(Stock))
    elif payload.ochirish:
        await session.execute(
            delete(Stock).where(Stock.tovar_uid.in_(payload.ochirish))
        )

    for q in payload.qoldiqlar:
        qiymatlar = {
            "tovar_uid": q.tovar_uid,
            "ombor": q.ombor,
            "tovar": q.tovar,
            "tovar_kodi": q.tovar_kodi,
            "shk": q.shk,
            "guruh": q.guruh,
            "birlik": q.birlik,
            "soni": round(float(q.soni or 0), 3),
            "tannarx": pul(q.tannarx),
            "sotish_narxi": pul(q.sotish_narxi),
            "yangilandi": datetime.now(timezone.utc),
        }
        stmt = pg_insert(Stock).values(**qiymatlar)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Stock.ombor, Stock.tovar_uid],
            set_={k: v for k, v in qiymatlar.items() if k not in ("ombor", "tovar_uid")},
        )
        await session.execute(stmt)

    await session.commit()
    log.info("Қолдиқ: %s янги/ўзгарган, %s ўчирилди%s",
             len(payload.qoldiqlar), len(payload.ochirish),
             " (тўлиқ сурат)" if payload.tozalash else "")
    return javob({
        "qabul": len(payload.qoldiqlar),
        "ochirildi": len(payload.ochirish),
    })


# ── Сайт: қолдиқ кўриш ва қидириш ─────────────────────


@app.get("/v1/qoldiq", dependencies=guard)
async def qoldiq(
    q: str | None = Query(None, max_length=100),
    ombor: str | None = Query(None, max_length=150),
    faqat_bor: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> dict:
    shart = []
    param: dict[str, Any] = {"lim": limit}

    if q:
        qq = q.strip()
        # Штрих код бўлса — аниқ мослик, акс ҳолда номи/коди бўйича қидирув
        if qq.isdigit() and len(qq) >= 6:
            shart.append("(shk = :aniq OR tovar_kodi = :aniq)")
            param["aniq"] = qq
        else:
            shart.append("(LOWER(tovar) LIKE :qidiruv OR tovar_kodi LIKE :qidiruv)")
            param["qidiruv"] = "%" + qq.lower() + "%"

    if ombor:
        shart.append("ombor = :ombor")
        param["ombor"] = ombor

    if faqat_bor:
        shart.append("soni <> 0")

    where = (" WHERE " + " AND ".join(shart)) if shart else ""

    rows = (
        await session.execute(
            text(
                f"""
                SELECT tovar_uid, ombor, tovar, tovar_kodi, shk, guruh, birlik,
                       soni, tannarx, sotish_narxi, yangilandi
                FROM stock {where}
                ORDER BY tovar
                LIMIT :lim
                """
            ),
            param,
        )
    ).all()

    jami = (
        await session.execute(
            text(
                """
                SELECT COUNT(*) AS pozitsiya,
                       COALESCE(SUM(soni * tannarx), 0) AS summa
                FROM stock WHERE soni <> 0
                """
            )
        )
    ).one()

    return javob(
        {
            "jami": {
                "pozitsiya": int(jami.pozitsiya or 0),
                "summa": round(float(jami.summa or 0)),
            },
            "royxat": [
                {
                    "uid": r.tovar_uid,
                    "ombor": r.ombor,
                    "tovar": r.tovar,
                    "kodi": r.tovar_kodi,
                    "shk": r.shk,
                    "guruh": r.guruh,
                    "birlik": r.birlik,
                    "soni": round(float(r.soni or 0), 3),
                    "tannarx": round(float(r.tannarx or 0)),
                    "sotish_narxi": round(float(r.sotish_narxi or 0)),
                    "yangilandi": r.yangilandi.strftime("%Y-%m-%d %H:%M") if r.yangilandi else "",
                }
                for r in rows
            ],
        }
    )


# ── Сайт: навбатга топшириқ қўйиш ─────────────────────


class InvSatrIn(BaseModel):
    tovar_uid: str = Field(default="", max_length=50)
    shk: str = Field(default="", max_length=30)
    tovar: str = Field(default="", max_length=300)
    fakt: float = 0
    hisobda: float = 0


class InvIn(BaseModel):
    ombor: str = Field(default="", max_length=150)
    izoh: str = Field(default="", max_length=300)
    satrlar: list[InvSatrIn] = []


@app.post("/v1/inventarizatsiya", dependencies=guard)
async def inventarizatsiya_yuborish(
    payload: InvIn,
    kim: str = Depends(check_auth),
    session: AsyncSession = Depends(get_session),
) -> dict:
    if not payload.satrlar:
        raise HTTPException(status_code=400, detail="Сатрлар бўш")

    n = Navbat(
        turi="inventarizatsiya",
        holat="kutmoqda",
        ombor=payload.ombor,
        izoh=payload.izoh,
        kim=kim,
        tana={
            "satrlar": [
                {
                    "tovar_uid": x.tovar_uid,
                    "shk": x.shk,
                    "tovar": x.tovar,
                    "fakt": round(float(x.fakt or 0), 3),
                    "hisobda": round(float(x.hisobda or 0), 3),
                }
                for x in payload.satrlar
            ]
        },
    )
    session.add(n)
    await session.commit()
    await session.refresh(n)

    log.info("Навбатга қўшилди: инвентаризация #%s, %s сатр", n.id, len(payload.satrlar))
    return javob({"id": n.id, "holat": n.holat, "satrlar": len(payload.satrlar)})


@app.get("/v1/inventarizatsiya", dependencies=guard)
async def inventarizatsiya_royxat(
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> dict:
    rows = (
        await session.execute(
            select(Navbat)
            .where(Navbat.turi == "inventarizatsiya")
            .order_by(Navbat.id.desc())
            .limit(limit)
        )
    ).scalars().all()

    return javob(
        {
            "royxat": [
                {
                    "id": n.id,
                    "holat": n.holat,
                    "ombor": n.ombor,
                    "izoh": n.izoh,
                    "kim": n.kim,
                    "satrlar": len((n.tana or {}).get("satrlar", [])),
                    "natija": n.natija,
                    "hujjat_raqami": n.hujjat_raqami,
                    "yaratildi": n.yaratildi.strftime("%Y-%m-%d %H:%M") if n.yaratildi else "",
                    "bajarildi": n.bajarildi.strftime("%Y-%m-%d %H:%M") if n.bajarildi else "",
                }
                for n in rows
            ]
        }
    )


# ── Таъминотчилар ─────────────────────────────────────


class TaminotchiIn(BaseModel):
    uid: str = Field(max_length=50)
    nomi: str = Field(default="", max_length=250)
    turi: str = Field(default="", max_length=50)
    telefon: str = Field(default="", max_length=50)


class TaminotchiSyncIn(BaseModel):
    royxat: list[TaminotchiIn] = []
    tozalash: bool = False


@app.post("/v1/sync/taminotchi", dependencies=guard)
async def sync_taminotchi(
    payload: TaminotchiSyncIn, session: AsyncSession = Depends(get_session)
) -> dict:
    if payload.tozalash:
        await session.execute(delete(Taminotchi))

    for t in payload.royxat:
        q = {
            "uid": t.uid,
            "nomi": t.nomi,
            "turi": t.turi,
            "telefon": t.telefon,
            "yangilandi": datetime.now(timezone.utc),
        }
        stmt = pg_insert(Taminotchi).values(**q)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Taminotchi.uid],
            set_={k: v for k, v in q.items() if k != "uid"},
        )
        await session.execute(stmt)

    await session.commit()
    log.info("Таъминотчи: %s та", len(payload.royxat))
    return javob({"qabul": len(payload.royxat)})


@app.get("/v1/taminotchi", dependencies=guard)
async def taminotchi_royxat(
    q: str | None = Query(None, max_length=100),
    limit: int = Query(50, ge=1, le=300),
    session: AsyncSession = Depends(get_session),
) -> dict:
    shart = ""
    param: dict[str, Any] = {"lim": limit}
    if q and q.strip():
        shart = " WHERE LOWER(nomi) LIKE :qq"
        param["qq"] = "%" + q.strip().lower() + "%"

    rows = (
        await session.execute(
            text(f"SELECT uid, nomi, turi, telefon FROM taminotchi{shart} "
                 "ORDER BY nomi LIMIT :lim"),
            param,
        )
    ).all()

    return javob({
        "royxat": [
            {"uid": r.uid, "nomi": r.nomi, "turi": r.turi, "telefon": r.telefon}
            for r in rows
        ]
    })


# ── Сайт: кирим ───────────────────────────────────────


class KirimSatrIn(BaseModel):
    tovar_uid: str = Field(default="", max_length=50)
    shk: str = Field(default="", max_length=30)
    tovar: str = Field(default="", max_length=300)
    soni: float = 0
    narxi: float = 0          # кирим нархи
    sotish_narxi: float = 0   # 0 бўлса — 1С эскисини қолдиради


class KirimIn(BaseModel):
    taminotchi: str = Field(default="", max_length=200)
    taminotchi_uid: str = Field(default="", max_length=50)
    ombor: str = Field(default="", max_length=150)
    izoh: str = Field(default="", max_length=300)
    satrlar: list[KirimSatrIn] = []


@app.post("/v1/kirim", dependencies=guard)
async def kirim_yuborish(
    payload: KirimIn,
    kim: str = Depends(check_auth),
    session: AsyncSession = Depends(get_session),
) -> dict:
    if not payload.satrlar:
        raise HTTPException(status_code=400, detail="Сатрлар бўш")

    n = Navbat(
        turi="kirim",
        holat="kutmoqda",
        ombor=payload.ombor,
        izoh=payload.izoh,
        kim=kim,
        tana={
            "taminotchi": payload.taminotchi,
            "taminotchi_uid": payload.taminotchi_uid,
            "satrlar": [
                {
                    "tovar_uid": x.tovar_uid,
                    "shk": x.shk,
                    "tovar": x.tovar,
                    "soni": round(float(x.soni or 0), 3),
                    "narxi": pul(x.narxi),
                    "sotish_narxi": pul(x.sotish_narxi),
                }
                for x in payload.satrlar
            ],
        },
    )
    session.add(n)
    await session.commit()
    await session.refresh(n)

    log.info("Навбатга қўшилди: кирим #%s, %s сатр", n.id, len(payload.satrlar))
    return javob({"id": n.id, "holat": n.holat, "satrlar": len(payload.satrlar)})


@app.get("/v1/kirim", dependencies=guard)
async def kirim_royxat(
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> dict:
    rows = (
        await session.execute(
            select(Navbat).where(Navbat.turi == "kirim")
            .order_by(Navbat.id.desc()).limit(limit)
        )
    ).scalars().all()

    return javob(
        {
            "royxat": [
                {
                    "id": n.id,
                    "holat": n.holat,
                    "taminotchi": (n.tana or {}).get("taminotchi", ""),
                    "ombor": n.ombor,
                    "izoh": n.izoh,
                    "kim": n.kim,
                    "satrlar": len((n.tana or {}).get("satrlar", [])),
                    "natija": n.natija,
                    "hujjat_raqami": n.hujjat_raqami,
                    "yaratildi": n.yaratildi.strftime("%Y-%m-%d %H:%M") if n.yaratildi else "",
                }
                for n in rows
            ]
        }
    )


# ── 1С: навбатни олиш ва ҳисобот бериш ────────────────


@app.get("/v1/navbat", dependencies=guard)
async def navbat_olish(
    limit: int = Query(5, ge=1, le=20),
    session: AsyncSession = Depends(get_session),
) -> dict:
    rows = (
        await session.execute(
            select(Navbat)
            .where(Navbat.holat == "kutmoqda")
            .order_by(Navbat.id)
            .limit(limit)
        )
    ).scalars().all()

    return javob(
        {
            "topshiriqlar": [
                {
                    "id": n.id,
                    "turi": n.turi,
                    "ombor": n.ombor,
                    "izoh": n.izoh,
                    "kim": n.kim,
                    "tana": n.tana or {},
                }
                for n in rows
            ]
        }
    )


class NatijaIn(BaseModel):
    holat: str = Field(default="bajarildi", max_length=20)
    natija: str = Field(default="", max_length=500)
    hujjat_raqami: str = Field(default="", max_length=50)


@app.post("/v1/navbat/{navbat_id}", dependencies=guard)
async def navbat_natija(
    navbat_id: int, payload: NatijaIn, session: AsyncSession = Depends(get_session)
) -> dict:
    n = await session.get(Navbat, navbat_id)
    if n is None:
        raise HTTPException(status_code=404, detail="Топшириқ топилмади")

    n.holat = payload.holat if payload.holat in ("bajarildi", "xato") else "xato"
    n.natija = payload.natija
    n.hujjat_raqami = payload.hujjat_raqami
    n.bajarildi = datetime.now(timezone.utc)
    await session.commit()

    log.info("Топшириқ #%s → %s: %s", navbat_id, n.holat, payload.natija[:80])
    return javob({"id": n.id, "holat": n.holat})


# ── Сайт: битта товар бўйича батафсил ─────────────────


@app.get("/v1/tovar", dependencies=guard)
async def tovar_hisoboti(
    nomi: str = Query(..., min_length=1, max_length=300),
    dan: str | None = Query(None),
    gacha: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
) -> dict:
    bugun = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    d1 = kun_oqi(dan, bugun.replace(day=1))
    d2 = kun_oqi(gacha, bugun) + timedelta(days=1)

    # Жами
    jami = (
        await session.execute(
            text(
                """
                SELECT COALESCE(SUM(soni), 0)    AS soni,
                       COALESCE(SUM(summa), 0)   AS savdo,
                       COALESCE(SUM(tannarx), 0) AS tannarx,
                       COALESCE(SUM(foyda), 0)   AS foyda,
                       COUNT(DISTINCT hujjat_uid) AS cheklar
                FROM sale_lines
                WHERE tovar = :nomi AND sana >= :d1 AND sana < :d2
                """
            ),
            {"nomi": nomi, "d1": d1, "d2": d2},
        )
    ).one()

    savdo = float(jami.savdo or 0)
    foyda = float(jami.foyda or 0)
    soni = float(jami.soni or 0)

    # Кунлар бўйича
    kunlar_rows = (
        await session.execute(
            text(
                """
                SELECT DATE(sana) AS kun,
                       SUM(soni)  AS soni,
                       SUM(summa) AS savdo,
                       SUM(foyda) AS foyda
                FROM sale_lines
                WHERE tovar = :nomi AND sana >= :d1 AND sana < :d2
                GROUP BY DATE(sana)
                ORDER BY kun DESC
                """
            ),
            {"nomi": nomi, "d1": d1, "d2": d2},
        )
    ).all()

    # Ҳар бир сотув — вақт, чек, сони, сумма
    amallar_rows = (
        await session.execute(
            text(
                """
                SELECT l.sana        AS vaqt,
                       l.soni        AS soni,
                       l.summa       AS summa,
                       l.foyda       AS foyda,
                       l.hujjat_uid  AS uid,
                       COALESCE(s.raqam, '') AS raqam,
                       COALESCE(s.sotuvchi, '') AS sotuvchi
                FROM sale_lines l
                LEFT JOIN sales s ON s.hujjat_uid = l.hujjat_uid
                WHERE l.tovar = :nomi AND l.sana >= :d1 AND l.sana < :d2
                ORDER BY l.sana DESC
                LIMIT 500
                """
            ),
            {"nomi": nomi, "d1": d1, "d2": d2},
        )
    ).all()

    # Товар коди ва гуруҳи — охирги сотувдан
    meta = (
        await session.execute(
            text(
                """
                SELECT tovar_kodi, guruh
                FROM sale_lines
                WHERE tovar = :nomi
                ORDER BY sana DESC
                LIMIT 1
                """
            ),
            {"nomi": nomi},
        )
    ).first()

    return javob(
        {
            "nomi": nomi,
            "kodi": (meta.tovar_kodi if meta else "") or "",
            "guruh": (meta.guruh if meta else "") or "",
            "dan": d1.strftime("%Y-%m-%d"),
            "gacha": (d2 - timedelta(days=1)).strftime("%Y-%m-%d"),
            "jami": {
                "soni": round(soni, 3),
                "savdo": round(savdo),
                "tannarx": round(float(jami.tannarx or 0)),
                "foyda": round(foyda),
                "cheklar": int(jami.cheklar or 0),
                "marja_foizi": round(foyda / savdo * 100, 1) if savdo else 0,
                "urtacha_narx": round(savdo / soni) if soni else 0,
            },
            "kunlar": [
                {
                    "kun": r.kun.strftime("%Y-%m-%d"),
                    "soni": round(float(r.soni or 0), 3),
                    "savdo": round(float(r.savdo or 0)),
                    "foyda": round(float(r.foyda or 0)),
                }
                for r in kunlar_rows
            ],
            "amallar": [
                {
                    "sana": r.vaqt.strftime("%Y-%m-%d"),
                    "vaqt": r.vaqt.strftime("%H:%M"),
                    "soni": round(float(r.soni or 0), 3),
                    "summa": round(float(r.summa or 0)),
                    "foyda": round(float(r.foyda or 0)),
                    "chek": r.raqam,
                    "uid": r.uid,
                    "sotuvchi": r.sotuvchi,
                }
                for r in amallar_rows
            ],
        }
    )


# ── Сайт ──────────────────────────────────────────────
# index.html шу репонинг ёнида туради. Аутентификация йўқ:
# саҳифанинг ўзи бўш, маълумот фақат /v1/... орқали келади.

SAYT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")


@app.get("/", include_in_schema=False)
async def sayt():
    if not os.path.exists(SAYT):
        return JSONResponse(
            status_code=404,
            content={"success": False, "message": "index.html топилмади", "data": {}},
        )
    return FileResponse(SAYT, media_type="text/html; charset=utf-8")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    # 204 да тана бўлмаслиги шарт — JSONResponse "null" ёзиб юборарди
    return Response(status_code=204)
