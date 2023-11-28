defmodule Magnitude.Data.WorkerTest do
  use ExUnit.Case

  alias Magnitude.Data
  alias Magnitude.DataSupervisor
  alias Magnitude.Schema.DataDefinition
  alias Magnitude.Schema.TableDefinition
  alias Magnitude.Schema.ColumnDefinition

  setup do
    {:ok, pid} = start_supervised({Magnitude.Supervisor, [data_directory: "./"]})
    %{pid: pid}
  end

  setup [:setup_table]

  test "starts a db worker", %{table_definition: table_definition} do
    {:ok, pid} = DataSupervisor.start_data_worker(table_definition)
    assert Process.alive?(pid)
  end

  test "finds a db worker based on table name", %{table_definition: table_definition} do
    {:ok, pid} = DataSupervisor.start_data_worker(table_definition)

    assert DataSupervisor.find_data_worker("cars") == pid
  end

  test "inserts a row", %{table_definition: table_definition} do
    {:ok, pid} = DataSupervisor.start_data_worker(table_definition)
    Data.insert(pid, %{"id" => 1234, "name" => "honda"})
  end

  def setup_table(_context) do
    columns = [
      ColumnDefinition.new("id", :int, %{nullable: false}),
      ColumnDefinition.new("name", :varchar, %{varchar_size: 20})
    ]

    table_definition = TableDefinition.new("cars", columns)
    %{table_definition: table_definition}
  end
end
