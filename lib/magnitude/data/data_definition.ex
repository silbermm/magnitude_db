defmodule Magnitude.Data.DataDefinition do
  @moduledoc """
  Defines the data for a table
  """
  alias __MODULE__
  alias Magnitude.Schema.ColumnDefinition
  alias Magnitude.Schema.TableDefinition

  @type column_value :: %{
          name: binary(),
          value: any()
        }

  @type t :: %DataDefinition{
          table_definition: TableDefinition.t(),
          key: binary() | number(),
          columns: list(column_value())
        }

  defstruct [:table_definition, :key, :columns]

  @spec new(binary() | number(), list(), TableDefinition.t()) :: DataDefinition.t()
  def new(key, columns, %TableDefinition{} = table_definition) do
    %DataDefinition{key: key, columns: columns, table_definition: table_definition}
  end

  @spec validate(DataDefinition.t()) :: :valid | {:invalid, atom()}
  def validate(%DataDefinition{columns: columns, table_definition: table_definition}) do
    Enum.reduce_while(columns, :valid, fn %{name: name, value: value}, acc ->
      column_definition = get_in(table_definition.columns, [name])

      case ColumnDefinition.validate(column_definition, value) do
        :valid -> {:cont, acc}
        err -> {:halt, err}
      end
    end)
  end
end
