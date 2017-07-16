package org.threadly.db.aurora.jdbi;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizer;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizerFactory;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizingAnnotation;
import org.skife.jdbi.v2.tweak.BaseStatementCustomizer;

@SuppressWarnings("rawtypes")
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
@SqlStatementCustomizingAnnotation(ReadOnly.Factory.class)
public @interface ReadOnly {
  public static class Factory implements SqlStatementCustomizerFactory {
    private static final SqlStatementCustomizer CUSTOMIZER = new SqlStatementCustomizer() {
      private final ReadOnlyCustomizer readOnlyCustomizer = new ReadOnlyCustomizer();

      @Override
      public void apply(SQLStatement stmt) throws SQLException {
        stmt.addStatementCustomizer(readOnlyCustomizer);
      }
    };

    @Override
    public SqlStatementCustomizer createForMethod(Annotation ann, Class c, Method meth) {
      return CUSTOMIZER;
    }

    @Override
    public SqlStatementCustomizer createForParameter(Annotation ann, Class c, Method meth,
                                                     Object param) {
      return CUSTOMIZER;
    }

    @Override
    public SqlStatementCustomizer createForType(Annotation ann, Class c) {
      return CUSTOMIZER;
    }

    // extending class has no logic or stored variables, so can be safely a singleton
    private static class ReadOnlyCustomizer extends BaseStatementCustomizer {
      @Override
      public void beforeExecution(PreparedStatement stmt,
                                  StatementContext ctx) throws SQLException {
        Connection c = ctx.getConnection();
        if (c != null) {
          c.setReadOnly(true);
        }
      }

      @Override
      public void cleanup(StatementContext ctx) {
        Connection c = ctx.getConnection();
        if (c != null) {
          try {
            c.setReadOnly(false);
          } catch (SQLException e) {
            // may be thrown if connection was closed
          }
        }
      }
    }
  }
}
