defmodule Magnitude.Schema.DataDefinitionTest do
  use ExUnit.Case, async: true

  alias Magnitude.Data.DataDefinition
  alias Magnitude.Schema.TableDefinition
  alias Magnitude.Schema.ColumnDefinition

  describe "success" do
    setup [:setup_table]

    test "new/3", %{table_definition: table_definition} do
      data = [
        %{name: "id", value: 1},
        %{name: "name", value: "crv"}
      ]

      data_definition = DataDefinition.new("id", data, table_definition)
      assert data_definition.key == "id"
      assert length(data_definition.columns) == 2
    end

    test "validate/2", %{table_definition: table_definition} do
      data = [
        %{name: "id", value: 1},
        %{name: "name", value: "crv"}
      ]

      data_definition = DataDefinition.new("id", data, table_definition)
      assert :valid == DataDefinition.validate(data_definition)
    end
  end

  describe "failure" do
    setup [:setup_table]

    test "validate/3 when invalid", %{table_definition: table_definition} do
      data = [
        %{name: "id", value: 1},
        %{name: "name", value: 120}
      ]

      data_definition = DataDefinition.new("id", data, table_definition)
      assert {:invalid, :invalid_varchar} == DataDefinition.validate(data_definition)
    end
  end

  defp setup_table(_context) do
    columns = [
      ColumnDefinition.new("id", :int, %{nullable: false}),
      ColumnDefinition.new("name", :varchar, %{varchar_size: 20})
    ]

    table_definition = TableDefinition.new("cars", columns)

    %{table_definition: table_definition}
  end
end
