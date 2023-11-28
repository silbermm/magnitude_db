defmodule Magnitude.Supervisor do
  @moduledoc """

  """

  use Supervisor
  alias Magnitude.Schema
  alias Magnitude.DataSupervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    # on startup of the DB
    # * find the existing schema table if it exists
    # * if not exists, create a new schema table
    children = [
      {Registry, keys: :unique, name: Magnitude.DataRegistry},
      {Schema, opts},
      {DataSupervisor, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
