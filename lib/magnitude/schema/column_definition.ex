defmodule Magnitude.Schema.ColumnDefinition do
  @moduledoc """
  Defines what a column in MagnitudeDB can be
  """
  alias __MODULE__

  defmodule Error do
    defexception [:message]
  end

  @db_types [:int, :bigint, :varchar, :float, :bool]
  @type db_type :: :int | :bigint | :varchar | :float | :bool

  @type varchar_size :: 0..255
  @type type_options :: %{
          optional(:varchar_size) => varchar_size,
          optional(:primary_key) => boolean(),
          nullable: boolean()
        }

  @typedoc """

  """
  @type t :: %ColumnDefinition{
          name: binary(),
          type: db_type(),
          type_options: type_options
        }

  defstruct [:name, :type, :type_options]

  @doc """
  Build a new column definition
  """
  @spec new(binary(), db_type(), type_options()) :: t()
  def new(name, :varchar, %{varchar_size: size} = opts) when size >= 0 and size <= 255 do
    type_opts = default_options(opts)
    %ColumnDefinition{name: name, type: :varchar, type_options: type_opts}
  end

  def new(_name, :varchar, _opts),
    do: raise(Error, message: "must define a varchar size when using a varchar")

  def new("", _type, _options) do
    raise Error, message: "name cannot be empty"
  end

  def new(name, type, type_opts) when is_binary(name) and type in @db_types do
    type_opts = default_options(type_opts)
    %ColumnDefinition{name: name, type: type, type_options: type_opts}
  end

  def new(name, _type, _type_opts) when is_binary(name) do
    raise(Error, message: "invalid column type - must be one of #{inspect(@db_types)}")
  end

  @spec default_options(type_options()) :: type_options()
  def default_options(type_opts) do
    type_opts
    |> Map.put_new(:nullable, true)
    |> Map.put_new(:primary_key, false)
  end

  @doc """
  Validate that a value is within the specification
  """
  @spec validate(t(), any()) :: :valid | {:invalid, atom()}
  def validate(%ColumnDefinition{type: type, type_options: type_opts}, value) do
    case type do
      :int ->
        if validate_int(value), do: :valid, else: {:invalid, :invalid_integer}

      :bigint ->
        if validate_bigint(value), do: :valid, else: {:invalid, :invalid_biginteger}

      :float ->
        if validate_float(value), do: :valid, else: {:invalid, :invalid_float}

      :varchar ->
        if validate_varchar(value, type_opts), do: :valid, else: {:invalid, :invalid_varchar}

      :bool ->
        if validate_bool(value), do: :valid, else: {:invalid, :invalid_boolean}
    end
  end

  defp validate_int(value), do: value >= -2_147_483_648 && value <= 2_147_483_647

  defp validate_bigint(value),
    do:
      is_integer(value) && value >= -9_223_372_036_854_775_808 &&
        value <= 9_223_372_036_854_775_807

  defp validate_float(value), do: is_float(value)
  defp validate_bool(value), do: is_boolean(value)

  defp validate_varchar(value, opts) do
    if is_binary(value) do
      graphemes = String.graphemes(value)
      length(graphemes) <= opts.varchar_size
    end
  end
end
