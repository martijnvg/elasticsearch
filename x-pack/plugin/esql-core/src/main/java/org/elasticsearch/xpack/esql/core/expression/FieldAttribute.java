/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.ESQL_AGGREGATE_DOUBLE_METRIC_FIELD;

/**
 * Attribute for an ES field.
 * To differentiate between the different type of fields this class offers:
 * - name - the fully qualified name (foo.bar.tar)
 * - path - the path pointing to the field name (foo.bar)
 * - parent - the immediate parent of the field; useful for figuring out the type of field (nested vs object)
 * - nestedParent - if nested, what's the parent (which might not be the immediate one)
 */
public class FieldAttribute extends TypedAttribute {
    // TODO: This constant should not be used if possible; use .synthetic()
    // https://github.com/elastic/elasticsearch/issues/105821
    public static final String SYNTHETIC_ATTRIBUTE_NAME_PREFIX = "$$";

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "FieldAttribute",
        FieldAttribute::new
    );

    private final FieldAttribute parent;
    private final String path;
    private final EsField field;

    private final NameId minSubNameId;
    private final NameId maxSubNameId;
    private final NameId sumSubNameId;
    private final NameId valueCountNameId;

    public FieldAttribute(Source source, String name, EsField field) {
        this(source, null, name, field);
    }

    public FieldAttribute(Source source, FieldAttribute parent, String name, EsField field) {
        this(source, parent, name, field, null, Nullability.TRUE, null, false);
    }

    public FieldAttribute(
        Source source,
        FieldAttribute parent,
        String name,
        EsField field,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        this(source, parent, name, field.getDataType(), field, qualifier, nullability, id, synthetic, null, null, null, null);
    }

    public FieldAttribute(
        Source source,
        FieldAttribute parent,
        String name,
        DataType type,
        EsField field,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        NameId minSubNameId,
        NameId maxSubNameId,
        NameId sumSubNameId,
        NameId valueCountNameId
    ) {
        super(source, name, type, qualifier, nullability, id, synthetic);
        this.path = parent != null ? parent.fieldName() : StringUtils.EMPTY;
        this.parent = parent;
        this.field = field;
        this.minSubNameId = minSubNameId;
        this.maxSubNameId = maxSubNameId;
        this.sumSubNameId = sumSubNameId;
        this.valueCountNameId = valueCountNameId;
    }

    public FieldAttribute(StreamInput in) throws IOException {
        /*
         * The funny casting dance with `(StreamInput & PlanStreamInput) in` is required
         * because we're in esql-core here and the real PlanStreamInput is in
         * esql-proper. And because NamedWriteableRegistry.Entry needs StreamInput,
         * not a PlanStreamInput. And we need PlanStreamInput to handle Source
         * and NameId. This should become a hard cast when we move everything out
         * of esql-core.
         */
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readOptionalWriteable(FieldAttribute::new),
            in.readString(),
            DataType.readFrom(in),
            in.readNamedWriteable(EsField.class),
            in.readOptionalString(),
            in.readEnum(Nullability.class),
            NameId.readFrom((StreamInput & PlanStreamInput) in),
            in.readBoolean(),
            in.getTransportVersion().onOrAfter(ESQL_AGGREGATE_DOUBLE_METRIC_FIELD)
                ? in.readOptionalWriteable(in1 -> NameId.readFrom((StreamInput & PlanStreamInput) in1))
                : null,
            in.getTransportVersion().onOrAfter(ESQL_AGGREGATE_DOUBLE_METRIC_FIELD)
                ? in.readOptionalWriteable(in1 -> NameId.readFrom((StreamInput & PlanStreamInput) in1))
                : null,
            in.getTransportVersion().onOrAfter(ESQL_AGGREGATE_DOUBLE_METRIC_FIELD)
                ? in.readOptionalWriteable(in1 -> NameId.readFrom((StreamInput & PlanStreamInput) in1))
                : null,
            in.getTransportVersion().onOrAfter(ESQL_AGGREGATE_DOUBLE_METRIC_FIELD)
                ? in.readOptionalWriteable(in1 -> NameId.readFrom((StreamInput & PlanStreamInput) in1))
                : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeOptionalWriteable(parent);
        out.writeString(name());
        dataType().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeOptionalString(qualifier());
        out.writeEnum(nullable());
        id().writeTo(out);
        out.writeBoolean(synthetic());
        if (out.getTransportVersion().onOrAfter(ESQL_AGGREGATE_DOUBLE_METRIC_FIELD)) {
            out.writeOptionalWriteable(maxSubNameId);
        }
        if (out.getTransportVersion().onOrAfter(ESQL_AGGREGATE_DOUBLE_METRIC_FIELD)) {
            out.writeOptionalWriteable(maxSubNameId);
        }
        if (out.getTransportVersion().onOrAfter(ESQL_AGGREGATE_DOUBLE_METRIC_FIELD)) {
            out.writeOptionalWriteable(sumSubNameId);
        }
        if (out.getTransportVersion().onOrAfter(ESQL_AGGREGATE_DOUBLE_METRIC_FIELD)) {
            out.writeOptionalWriteable(valueCountNameId);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<FieldAttribute> info() {
        return NodeInfo.create(
            this,
            (source, parent, name, dataType, esField, qualifier, nullability, nameId, synthetic) -> new FieldAttribute(
                source,
                parent,
                name,
                dataType,
                esField,
                qualifier,
                nullability,
                nameId,
                synthetic,
                null,
                null,
                null,
                null
            ),
            parent,
            name(),
            dataType(),
            field,
            qualifier(),
            nullable(),
            id(),
            synthetic()
        );
    }

    public FieldAttribute parent() {
        return parent;
    }

    public String path() {
        return path;
    }

    /**
     * The full name of the field in the index, including all parent fields. E.g. {@code parent.subfield.this_field}.
     */
    public String fieldName() {
        // Before 8.15, the field name was the same as the attribute's name.
        // On later versions, the attribute can be renamed when creating synthetic attributes.
        // TODO: We should use synthetic() to check for that case.
        // https://github.com/elastic/elasticsearch/issues/105821
        if (name().startsWith(SYNTHETIC_ATTRIBUTE_NAME_PREFIX) == false) {
            return name();
        }
        return Strings.hasText(path) ? path + "." + field.getName() : field.getName();
    }

    public String qualifiedPath() {
        // return only the qualifier is there's no path
        return qualifier() != null ? qualifier() + (Strings.hasText(path) ? "." + path : StringUtils.EMPTY) : path;
    }

    public EsField.Exact getExactInfo() {
        return field.getExactInfo();
    }

    public FieldAttribute exactAttribute() {
        EsField exactField = field.getExactField();
        if (exactField.equals(field) == false) {
            return innerField(exactField);
        }
        return this;
    }

    private FieldAttribute innerField(EsField type) {
        return new FieldAttribute(source(), this, name() + "." + type.getName(), type, qualifier(), nullable(), id(), synthetic());
    }

    @Override
    protected Attribute clone(
        Source source,
        String name,
        DataType type,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        FieldAttribute qualifiedParent = parent != null ? (FieldAttribute) parent.withQualifier(qualifier) : null;
        return new FieldAttribute(
            source,
            qualifiedParent,
            name,
            field.getDataType(),
            field,
            qualifier,
            nullability,
            id,
            synthetic,
            minSubNameId,
            maxSubNameId,
            sumSubNameId,
            valueCountNameId
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, field);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && Objects.equals(path, ((FieldAttribute) obj).path)
            && Objects.equals(field, ((FieldAttribute) obj).field);
    }

    @Override
    protected String label() {
        return "f";
    }

    public EsField field() {
        return field;
    }

    public static FieldAttribute createAggregatedFieldAttribute(Source source, String name, EsField field) {
        return new FieldAttribute(
            source,
            null,
            name,
            DataType.AGGREGATE_DOUBLE_METRIC,
            field,
            null,
            Nullability.TRUE,
            null,
            false,
            new NameId(),
            new NameId(),
            new NameId(),
            new NameId()
        );
    }

    public FieldAttribute getAggregatedMinSubField() {
        return createMetricSubAttribute("min", minSubNameId);
    }

    public FieldAttribute getAggregatedMaxSubField() {
        return createMetricSubAttribute("max", maxSubNameId);
    }

    public FieldAttribute getAggregatedSumSubField() {
        return createMetricSubAttribute("sum", sumSubNameId);
    }

    public FieldAttribute getAggregatedValueCountSubField() {
        return createMetricSubAttribute("value_count", valueCountNameId);
    }

    public boolean isAggregatedAttribute() {
        return dataType() == DataType.AGGREGATE_DOUBLE_METRIC;
    }

    public boolean isAggregateSubAttribute() {
        return parent != null && parent.isAggregatedAttribute();
    }

    public String metric() {
        assert isAggregateSubAttribute();
        int lastDotIndex = name().lastIndexOf('.');
        assert lastDotIndex > 0;
        return name().substring(lastDotIndex + 1);
    }

    private FieldAttribute createMetricSubAttribute(String metric, NameId nameId) {
        var esField = new EsField(metric, "value_count".equals(metric) ? DataType.INTEGER : DataType.DOUBLE, Map.of(), true);
        return new FieldAttribute(source(), this, name() + "." + metric, esField, null, Nullability.TRUE, nameId, false);
    }

}
