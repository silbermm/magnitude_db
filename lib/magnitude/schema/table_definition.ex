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
          indexes: list()
        }

  defmodule Error do
    defexception [:message]
  end

  defstruct [:name, :columns, :indexes]

  @spec new(binary(), list(ColumnDefinition.t())) :: TableDefinition.t()
  def new(name, columns) do
    table = %TableDefinition{name: name, columns: %{}}
    Enum.reduce(columns, table, &add_column/2)
  end

  @spec add_column(ColumnDefinition.t(), TableDefinition.t()) :: TableDefinition.t()
  def add_column(%ColumnDefinition{} = column, table) do
    %{table | columns: Map.put(table.columns, column.name, column)}
  end
end
