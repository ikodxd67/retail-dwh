"""Модель Data Vault из models/vault.yml и проверка её целостности."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from retail_dwh.settings import PROJECT_DIR

SCHEMA = "raw_vault"


class ModelError(ValueError):
    pass


@dataclass(frozen=True)
class Hub:
    name: str
    business_key: str

    @property
    def table(self) -> str:
        return f"hub_{self.name}"

    @property
    def hk(self) -> str:
        return f"hk_{self.name}"


@dataclass(frozen=True)
class Link:
    name: str
    hubs: tuple[str, ...]
    dependent_key: str | None = None
    columns: dict[str, str] = field(default_factory=dict)  # только у неисторизуемых
    non_historized: bool = False

    @property
    def table(self) -> str:
        return self.name if self.non_historized else f"lnk_{self.name}"

    @property
    def hk(self) -> str:
        return f"hk_{self.name}"


@dataclass(frozen=True)
class Satellite:
    name: str
    parent: str  # хаб или линк
    columns: dict[str, str]
    pii: bool = False


@dataclass(frozen=True)
class SourceMapping:
    table: str
    record_source: str
    applied_ts: str
    keys: dict[str, str]
    dependent_keys: dict[str, str]
    links: tuple[str, ...]
    satellites: dict[str, dict[str, str]]
    non_historized_links: dict[str, dict[str, str]]


@dataclass(frozen=True)
class Reference:
    source_table: str
    table: str
    record_source: str
    key: tuple[str, ...]
    columns: dict[str, str]
    mapping: dict[str, str]


@dataclass(frozen=True)
class VaultModel:
    hubs: dict[str, Hub]
    links: dict[str, Link]
    satellites: dict[str, Satellite]
    sources: dict[str, SourceMapping]
    references: dict[str, Reference] = field(default_factory=dict)

    @property
    def loadable_tables(self) -> list[str]:
        return [*self.sources, *self.references]

    def parent_hk(self, sat: Satellite) -> str:
        return f"hk_{sat.parent}"

    def validate(self) -> None:
        errors = []
        for link in self.links.values():
            for hub in link.hubs:
                if hub not in self.hubs:
                    errors.append(f"линк {link.name}: нет хаба {hub}")
        for sat in self.satellites.values():
            if sat.parent not in self.hubs and sat.parent not in self.links:
                errors.append(f"сателлит {sat.name}: нет родителя {sat.parent}")
        for src in self.sources.values():
            for hub in src.keys:
                if hub not in self.hubs:
                    errors.append(f"{src.table}: ключ для неизвестного хаба {hub}")
            for name in (*src.links, *src.non_historized_links):
                link = self.links.get(name)
                if link is None:
                    errors.append(f"{src.table}: неизвестный линк {name}")
                    continue
                missing = [h for h in link.hubs if h not in src.keys]
                if missing:
                    errors.append(f"{src.table}: для линка {name} не хватает ключей {missing}")
                if link.dependent_key and link.dependent_key not in src.dependent_keys:
                    errors.append(f"{src.table}: для линка {name} нужен {link.dependent_key}")
            for sat_name, cols in src.satellites.items():
                sat = self.satellites.get(sat_name)
                if sat is None:
                    errors.append(f"{src.table}: неизвестный сателлит {sat_name}")
                    continue
                if sat.parent in self.hubs and sat.parent not in src.keys:
                    errors.append(f"{src.table}: у {sat_name} нет ключа родителя {sat.parent}")
                if sat.parent in self.links and sat.parent not in src.links:
                    errors.append(
                        f"{src.table}: {sat_name} висит на линке {sat.parent}, который источник не грузит"
                    )
                if set(cols) != set(sat.columns):
                    errors.append(
                        f"{src.table}: колонки {sat_name} не совпадают с моделью: "
                        f"{sorted(set(cols) ^ set(sat.columns))}"
                    )
            for name, cols in src.non_historized_links.items():
                link = self.links.get(name)
                if link and set(cols) != set(link.columns):
                    errors.append(f"{src.table}: колонки {name} не совпадают с моделью")
        for ref in self.references.values():
            if set(ref.mapping) != set(ref.columns):
                errors.append(f"{ref.source_table}: mapping не совпадает с columns")
            if not set(ref.key) <= set(ref.columns):
                errors.append(f"{ref.source_table}: ключ не из колонок")
        if errors:
            raise ModelError("; ".join(errors))


def load_model(path: Path | None = None) -> VaultModel:
    raw = yaml.safe_load((path or PROJECT_DIR / "models/vault.yml").read_text(encoding="utf-8"))
    hubs = {name: Hub(name, spec["business_key"]) for name, spec in raw["hubs"].items()}
    links = {
        name: Link(name, tuple(spec["hubs"]), spec.get("dependent_key"))
        for name, spec in raw.get("links", {}).items()
    }
    for name, spec in raw.get("non_historized_links", {}).items():
        links[name] = Link(
            name, tuple(spec["hubs"]), spec.get("dependent_key"), dict(spec["columns"]), non_historized=True
        )
    sats = {
        name: Satellite(name, spec["parent"], dict(spec["columns"]), bool(spec.get("pii")))
        for name, spec in raw.get("satellites", {}).items()
    }
    sources = {
        table: SourceMapping(
            table=table,
            record_source=spec["record_source"],
            applied_ts=spec["applied_ts"],
            keys=dict(spec.get("keys", {})),
            dependent_keys=dict(spec.get("dependent_keys", {})),
            links=tuple(spec.get("links", [])),
            satellites={k: dict(v) for k, v in spec.get("satellites", {}).items()},
            non_historized_links={k: dict(v) for k, v in spec.get("non_historized_links", {}).items()},
        )
        for table, spec in raw["sources"].items()
    }
    references = {
        table: Reference(
            table,
            spec["table"],
            spec["record_source"],
            tuple(spec["key"]),
            dict(spec["columns"]),
            dict(spec["mapping"]),
        )
        for table, spec in raw.get("references", {}).items()
    }
    model = VaultModel(hubs, links, sats, sources, references)
    model.validate()
    return model
