defmodule Magnitude.DataSupervisor do
  use DynamicSupervisor

  alias Magnitude.Data

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one, extra_arguments: [init_arg])
  end

  def start_data_worker(table) do
    spec = {Data, table_definition: table}
    DynamicSupervisor.start_child(__MODULE__, spec) 
  end

  def find_data_worker(table) do
    case Registry.lookup(Magnitude.DataRegistry, table) do
      [{pid, _}] when is_pid(pid) -> pid
      _ -> nil
    end
  end
end
