defmodule Magnitude.Data do
  use GenServer

  alias Magnitude.Data.DataDefinition

  def start_link(init_opts, opts) do
    opts = Keyword.merge(init_opts, opts)
    table_definition = Keyword.get(opts, :table_definition)
    GenServer.start_link(__MODULE__, opts, name: name(table_definition.name))
  end

  def init(opts) do
    data_directory = Keyword.get(opts, :data_directory)
    table_definition = Keyword.get(opts, :table_definition)

    {:ok, %{data_directory: data_directory, table_definition: table_definition, data: []}}
  end

  def insert(pid, data), do: GenServer.call(pid, {:insert, data})

  def handle_call({:insert, data}, _from, state) do
    column_data =
      for {key, value} <- data do
        %{name: key, value: value}
      end

    data_definition = DataDefinition.new("", column_data, state.table_definition)

    case DataDefinition.validate(data_definition) do
      :valid ->
        # insert it
        state = append(data, state)
        {:reply, {:ok, data}, state}

      {:invalid, _reason} = err ->
        {:reply, err, state}
    end
  end

  defp append(data, state) do
    %{state | data: state.data ++ [data]}
  end

  defp name(table_name) do
    {:via, Registry, {Magnitude.DataRegistry, table_name}}
  end
end
