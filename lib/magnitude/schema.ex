defmodule Magnitude.Schema do
  use GenServer

  alias Magnitude.DataSupervisor
  alias Magnitude.Schema.ColumnDefinition
  alias Magnitude.Schema.DataDefinition
  alias Magnitude.Schema.TableDefinition
  alias __MODULE__

  @type column :: %{
          name: binary(),
          type: ColumnDefinition.db_type(),
          options: ColumnDefinition.type_options()
        }

  defstruct [:data_directory, :data, :tables]

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    data_dir = Keyword.get(opts, :data_directory)
    schema = %Schema{data_directory: data_dir, data: [], tables: %{}}
    {:ok, schema, {:continue, :maybe_create_schema}}
  end

  @spec create_table(TableDefinition.t()) :: :ok
  def create_table(table_definition) do
    GenServer.call(__MODULE__, {:create_table, table_definition})
  end

  def describe_table(table) do
    GenServer.call(__MODULE__, {:describe_table, table})
  end

  @spec insert(DataDefinition.t()) :: map()
  def insert(data), do: GenServer.call(__MODULE__, {:insert, data})

  @impl true
  def handle_call({:create_table, table_definition}, _from, %{tables: tables} = schema) do
    if Map.has_key?(tables, table_definition.name) do
      {:reply, {:error, :already_exists}, schema}
    else
      updated_tables = Map.put(tables, table_definition.name, table_definition)
      DataSupervisor.start_data_worker(table_definition)
      {:reply, :ok, %{schema | tables: updated_tables}}
    end
  end

  @impl true
  def handle_call({:describe_table, table_name}, _from, %{tables: tables} = schema) do
    table = Map.get(tables, table_name, nil)

    if table do
      columns = Map.get(table, :columns, [])
      indexes = Map.get(table, :indexes, [])

      simplified_columns =
        for {_id, definition} <- columns do
          Map.from_struct(definition)
        end

      simplified_indexes =
        for {type, column} <- indexes do
          {type, column.name}
        end
        |> Map.new()

      ret = %{
        name: table_name,
        columns: simplified_columns,
        indexes: simplified_indexes
      }

      {:reply, ret, schema}
    else
      {:reply, nil, schema}
    end
  end

  @impl true
  def handle_continue(:maybe_create_schema, state) do
    IO.inspect("creating data dir (maybe)")
    {:noreply, state}
  end
end
