defmodule MagnitudeTest do
  use ExUnit.Case
  doctest Magnitude

  alias Magnitude.Schema.ColumnDefinition

  setup do
    {:ok, pid} = start_supervised({Magnitude.Supervisor, [data_directory: "./"]})
    %{pid: pid}
  end

  setup [:setup_table]

  test "create a table", %{pid: pid} do
    assert :ok ==
             Magnitude.create_table("new_table", [
               {"id", :int, %{nullable: false}}
             ])
  end

  describe "unable to create table" do
    test "when table already exists", %{pid: pid} do
      {:error, :already_exists} =
        Magnitude.create_table("testing_table", [{"id", :int, %{nullable: false}}])
    end

    test "when columns are incorrectly defined" do
      assert_raise(Magnitude.Schema.ColumnDefinition.Error, fn ->
        Magnitude.create_table("new_table", [{"id", :blah, %{nullable: false}}])
      end)
    end
  end

  defp setup_table(%{pid: pid}) do
    Magnitude.create_table("testing_table", [{"id", :int, %{nullable: false, primary_key: true}}])
  end
end
