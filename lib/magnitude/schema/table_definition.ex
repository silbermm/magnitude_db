defmodule Magnitude.Schema.TableDefinition do
  @moduledoc """

  """
  alias __MODULE__
  alias Magnitude.Schema.ColumnDefinition

  @type t :: %TableDefinition{
          name: binary(),
          columns: %{
            optional(atom()) => ColumnDefinition.t()
          },
          indexes: map()
        }

  defmodule Error do
    defexception [:message]
  end

  defstruct [:name, :columns, :indexes]

  @spec new(binary(), list(ColumnDefinition.t())) :: TableDefinition.t()
  def new(name, columns) do
    table = %TableDefinition{name: name, columns: %{}, indexes: %{}}
    Enum.reduce(columns, table, &add_column/2)
  end

  @spec add_column(ColumnDefinition.t(), TableDefinition.t()) :: TableDefinition.t()
  def add_column(%ColumnDefinition{} = column, table) do
    if column.type_options.primary_key do
      %{table | columns: Map.put(table.columns, column.name, column), indexes: Map.put(table.indexes, :primary_key, column)} 
    else
      %{table | columns: Map.put(table.columns, column.name, column)}
    end
  end

  @spec get_primary_key(TableDefinition.t()) :: ColumnDefinition.t()
  def get_primary_key(table_defintion) do
    Map.get(table_defintion.indexes, :primary_key)
  end
end
