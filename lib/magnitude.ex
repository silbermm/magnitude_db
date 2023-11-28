defmodule Magnitude do
  @moduledoc """
  Simple multi-column database
  """
  alias Magnitude.Data
  alias Magnitude.DataSupervisor
  alias Magnitude.Schema
  alias Magnitude.Schema.ColumnDefinition
  alias Magnitude.Schema.TableDefinition

  @doc """
  Create a new table with the defined columns.

  ## Example

      iex> alias Magnitude.Schema.ColumnDefinition
      iex> columns = [
      ...>   {"id", :int, %{nullable: false, primary_key: true}},
      ...>   {"make", :varchar, %{varchar_size: 50}},
      ...>   {"model", :varchar, %{varchar_size: 50}}
      ...> ]
      iex> Magnitude.create_table("cars", columns)
      :ok
  """
  @spec create_table(binary(), list(tuple())) :: :ok | {:error, atom()}
  def create_table(table_name, columns) do
    columns = for {name, type, opts} <- columns do
      ColumnDefinition.new(name, type, opts)
    end

    table_definition = TableDefinition.new(table_name, columns)
    Schema.create_table(table_definition)
  end

  @doc """
  Returns the table definition if it exists.

  ## Example

      iex> Magnitude.describe("testing_table")
      %{name: "testing_table", columns: [%{name: "id", type: :int, type_options: %{nullable: false, primary_key: true}}], indexes: nil}

      iex> Magnitude.describe("non-existent-table")
      nil
  """
  @spec describe(binary()) :: map()
  def describe(table) do
    Schema.describe_table(table)
  end


  @doc """
  Inserts data into a table if the table exists

  ## Example

      iex> Magnitude.insert("testing_table", %{"id" => 1234})
      {:ok, %{"id" => 1234}}
  """
  @spec insert(binary(), map()) :: {:ok, map()} | {:error, atom()}
  def insert(table, data) do
    # get data worker
    case DataSupervisor.find_data_worker(table) do
      pid when is_pid(pid) -> 
        Data.insert(pid, data)
      _ -> 
        {:error, :noexist}
    end
  end

  #   def update(table, data) do
  #   end

  #   def index(table, index_description) do
  #   end

  #   def select(table, sql) do
  #   end
end
