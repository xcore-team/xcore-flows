
from alembic import context
import importlib.util
from pathlib import Path


def _models():
    """Les modèles du plugin, chargés par leur CHEMIN. L'ancien `from app.xflows.src.repositories import *` exigeait que le plugin
    vive sous `app/xflows` (et sous ce nom) : ailleurs — livré avec xcore-doctype, posé par `xcore-doctype install` — la migration
    échouait, et en production (`MODE=production`) le plugin refusait de démarrer. `models.py` n'importe que SQLAlchemy."""
    import sys

    name = "_xflow_migration_models"
    if name in sys.modules:  # une migration rejouée dans le même processus : les mêmes classes, pas une seconde déclaration
        return sys.modules[name]
    path = Path(__file__).resolve().parents[1] / "src" / "repositories" / "models.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # SQLAlchemy résout les annotations (`Mapped[...]`) par le module : il doit y être
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


target_metadata = _models().Base.metadata

# Table de version PROPRE à ce plugin. Elle était partagée (`alembic_version`, une seule ligne) entre
# tous les plugins : la révision de l'un écrasait ou rendait illisible celle des autres.
VERSION_TABLE = "alembic_version_xflows"


def _adopt_legacy_version(connection) -> None:
    """Transition : recopie dans VERSION_TABLE la révision de CE plugin trouvée dans l'ancienne table
    partagée `alembic_version`, sans rejouer de migration. Idempotent ; l'ancienne table n'est jamais
    modifiée ni supprimée (aucune donnée perdue, retour arrière possible)."""
    from alembic.script import ScriptDirectory
    from sqlalchemy import inspect, text

    insp = inspect(connection)
    if not insp.has_table("alembic_version"):
        return
    if insp.has_table(VERSION_TABLE):
        if connection.execute(text(f"SELECT COUNT(*) FROM {VERSION_TABLE}")).scalar():
            return
    known = {r.revision for r in ScriptDirectory.from_config(context.config).walk_revisions()}
    mine = [v for (v,) in connection.execute(text("SELECT version_num FROM alembic_version")) if v in known]
    # Des identifiants comme « 0001_initial » existent dans plusieurs plugins : on n'adopte que si les
    # tables de CE plugin existent déjà (preuve que ses migrations ont réellement été jouées).
    if not mine or not any(insp.has_table(t) for t in target_metadata.tables):
        return
    if not insp.has_table(VERSION_TABLE):
        connection.execute(text(
            f"CREATE TABLE {VERSION_TABLE} (version_num VARCHAR(32) NOT NULL, "
            f"CONSTRAINT {VERSION_TABLE}_pkc PRIMARY KEY (version_num))"
        ))
    for version in mine:
        connection.execute(text(f"INSERT INTO {VERSION_TABLE} (version_num) VALUES (:v)"), {"v": version})


def run_migrations_offline() -> None:
    url = context.config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, version_table=VERSION_TABLE, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = context.config.attributes.get("connection")
    if connectable is None:
        from sqlalchemy import engine_from_config, pool
        connectable = engine_from_config(
            context.config.get_section(context.config.config_ini_section, {}),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )
        with connectable.connect() as connection:
            _adopt_legacy_version(connection)
            connection.commit()
            context.configure(connection=connection, target_metadata=target_metadata, version_table=VERSION_TABLE)
            with context.begin_transaction():
                context.run_migrations()
    else:
        _adopt_legacy_version(connectable)
        context.configure(connection=connectable, target_metadata=target_metadata, version_table=VERSION_TABLE)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
