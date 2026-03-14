from pathlib import Path

from ffxiahbot.config import Config


def test_from_yaml_loads_seller_pool_from_external_file(tmp_path: Path) -> None:
    (tmp_path / "config.yaml").write_text(
        "\n".join(
            [
                "name: M.H.M.U.",
                "seller_pool_path: seller_personae.yaml",
            ]
        )
    )
    (tmp_path / "seller_personae.yaml").write_text(
        "\n".join(
            [
                "seller_pool:",
                "  - id: 101",
                "    name: Cloud",
                "    weight: 2.0",
                "  - id: 102",
                "    name: Cecil",
                "    weight: 1.0",
            ]
        )
    )

    config = Config.from_yaml(tmp_path / "config.yaml")

    assert config.seller_pool is not None
    assert [persona.id for persona in config.seller_pool] == [101, 102]
    assert [persona.name for persona in config.seller_pool] == ["Cloud", "Cecil"]


def test_from_yaml_prefers_inline_seller_pool_over_external_file(tmp_path: Path) -> None:
    (tmp_path / "config.yaml").write_text(
        "\n".join(
            [
                "name: M.H.M.U.",
                "seller_pool_path: seller_personae.yaml",
                "seller_pool:",
                "  - id: 201",
                "    name: Terra",
                "    weight: 3.0",
            ]
        )
    )
    (tmp_path / "seller_personae.yaml").write_text(
        "\n".join(
            [
                "seller_pool:",
                "  - id: 202",
                "    name: Noctis",
                "    weight: 1.0",
            ]
        )
    )

    config = Config.from_yaml(tmp_path / "config.yaml")

    assert config.seller_pool is not None
    assert [persona.id for persona in config.seller_pool] == [201]
    assert [persona.name for persona in config.seller_pool] == ["Terra"]
