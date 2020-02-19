package org.threadly.db.aurora.jdbi;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;

import org.jdbi.v3.core.statement.SqlStatement;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.statement.StatementCustomizer;
import org.jdbi.v3.sqlobject.customizer.SqlStatementCustomizer;
import org.jdbi.v3.sqlobject.customizer.SqlStatementCustomizerFactory;
import org.jdbi.v3.sqlobject.customizer.SqlStatementCustomizingAnnotation;

@SuppressWarnings("rawtypes")
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
@SqlStatementCustomizingAnnotation(ReadOnly.Factory.class)
public @interface ReadOnly {
  public static class Factory implements SqlStatementCustomizerFactory {
    private static final SqlStatementCustomizer CUSTOMIZER =
        new SqlStatementCustomizer() {
          private final ReadOnlyCustomizer customizer = new ReadOnlyCustomizer();
    
          @Override
          public void apply(SqlStatement<?> stmt) throws SQLException {
            stmt.addCustomizer(customizer);
          }
        };

    @Override
    public SqlStatementCustomizer createForMethod(Annotation ann, Class c, Method meth) {
      return CUSTOMIZER;
    }

    @Override
    public SqlStatementCustomizer createForType(Annotation ann, Class c) {
      return CUSTOMIZER;
    }
  }
  
  public static class ReadOnlyCustomizer implements StatementCustomizer {
    @Override
    public void beforeExecution(PreparedStatement stmt,
                                StatementContext ctx) throws SQLException {
      Connection c = ctx.getConnection();
      if (c != null) {
        try {
          c.setReadOnly(true);
        } catch (SQLClientInfoException e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public void afterExecution(PreparedStatement stmt, StatementContext ctx) {
      Connection c = ctx.getConnection();
      if (c != null) {
        try {
          c.setReadOnly(false);
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
