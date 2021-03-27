/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.types

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * A non-concrete data type, reserved for internal uses.
 */
private[sql] abstract class AbstractDataType {
  /**
   * The default concrete type to use if we want to cast a null literal into this type.
   */
  // 默认的具体类型
  // 当强制转换一个 null 字面量变成这个 DataType 的时候，转换成的具体 DataType
  private[sql] def defaultConcreteType: DataType

  /**
   * Returns true if `other` is an acceptable input type for a function that expects this,
   * possibly abstract DataType.
   *
   * {{{
   *   // this should return true
   *   DecimalType.acceptsType(DecimalType(10, 2))
   *
   *   // this should return true as well
   *   NumericType.acceptsType(DecimalType(10, 2))
   * }}}
   */
  // 个人理解，就是这个 DataType 能和什么其他什么类型的 DataType 兼容
  // 如果能兼容就返回 true
  // 似乎一般都是 父类 可以兼容 子类
  private[sql] def acceptsType(other: DataType): Boolean

  /** Readable string representation for the type. */
  // DataType 的特定表示格式
  private[sql] def simpleString: String
}


/**
 * A collection of types that can be used to specify type constraints. The sequence also specifies
 * precedence: an earlier type takes precedence over a latter type.
 *
 * {{{
 *   TypeCollection(StringType, BinaryType)
 * }}}
 *
 * This means that we prefer StringType over BinaryType if it is possible to cast to StringType.
 */
private[sql] class TypeCollection(private val types: Seq[AbstractDataType])
  // 集合类型的 DataType
  extends AbstractDataType {

  require(types.nonEmpty, s"TypeCollection ($types) cannot be empty")

  // 返回的是第一个元素的 defaultConcreteType
  override private[sql] def defaultConcreteType: DataType = types.head.defaultConcreteType

  // 每一个元素都要被 accept
  override private[sql] def acceptsType(other: DataType): Boolean =
    types.exists(_.acceptsType(other))

  // 最终的格式是 (A or B or C)
  // mkString 的用法就是这样的
  override private[sql] def simpleString: String = {
    types.map(_.simpleString).mkString("(", " or ", ")")
  }
}


private[sql] object TypeCollection {

  /**
   * Types that can be ordered/compared. In the long run we should probably make this a trait
   * that can be mixed into each data type, and perhaps create an [[AbstractDataType]].
   */
  // TODO: Should we consolidate this with RowOrdering.isOrderable?
  val Ordered = TypeCollection(
    BooleanType,
    ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, DecimalType,
    TimestampType, DateType,
    StringType, BinaryType)

  /**
   * Types that include numeric types and interval type. They are only used in unary_minus,
   * unary_positive, add and subtract operations.
   */
  val NumericAndInterval = TypeCollection(NumericType, CalendarIntervalType)

  def apply(types: AbstractDataType*): TypeCollection = new TypeCollection(types)

  def unapply(typ: AbstractDataType): Option[Seq[AbstractDataType]] = typ match {
    case typ: TypeCollection => Some(typ.types)
    case _ => None
  }
}


/**
 * An [[AbstractDataType]] that matches any concrete data types.
 */
protected[sql] object AnyDataType extends AbstractDataType {
  // AnyDataType 可以对应任何类型
  // 这里是 object extends 了一个 Abstract 的 DataType

  // Note that since AnyDataType matches any concrete types, defaultConcreteType should never
  // be invoked.
  override private[sql] def defaultConcreteType: DataType = throw new UnsupportedOperationException

  override private[sql] def simpleString: String = "any"

  override private[sql] def acceptsType(other: DataType): Boolean = true
}


/**
 * An internal type used to represent everything that is not null, UDTs, arrays, structs, and maps.
 */
protected[sql] abstract class AtomicType extends DataType {
  private[sql] type InternalType
  private[sql] val tag: TypeTag[InternalType]
  private[sql] val ordering: Ordering[InternalType]
}


/**
 * Numeric data types.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
abstract class NumericType extends AtomicType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization. In order for
  // implicitly[Numeric[JvmType]] to be valid, we have to change JvmType from a type variable to a
  // type parameter and add a numeric annotation (i.e., [JvmType : Numeric]). This gets
  // desugared by the compiler into an argument to the objects constructor. This means there is no
  // longer a no argument constructor and thus the JVM cannot serialize the object anymore.
  private[sql] val numeric: Numeric[InternalType]
}


private[sql] object NumericType extends AbstractDataType {
  /**
   * Enables matching against NumericType for expressions:
   * {{{
   *   case Cast(child @ NumericType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[NumericType]

  override private[sql] def defaultConcreteType: DataType = DoubleType

  override private[sql] def simpleString: String = "numeric"

  override private[sql] def acceptsType(other: DataType): Boolean = other.isInstanceOf[NumericType]
}


private[sql] object IntegralType extends AbstractDataType {
  /**
   * Enables matching against IntegralType for expressions:
   * {{{
   *   case Cast(child @ IntegralType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[IntegralType]

  override private[sql] def defaultConcreteType: DataType = IntegerType

  override private[sql] def simpleString: String = "integral"

  override private[sql] def acceptsType(other: DataType): Boolean = other.isInstanceOf[IntegralType]
}


private[sql] abstract class IntegralType extends NumericType {
  private[sql] val integral: Integral[InternalType]
}


private[sql] object FractionalType {
  /**
   * Enables matching against FractionalType for expressions:
   * {{{
   *   case Cast(child @ FractionalType(), StringType) =>
   *     ...
   * }}}
   */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[FractionalType]
}


private[sql] abstract class FractionalType extends NumericType {
  private[sql] val fractional: Fractional[InternalType]
  private[sql] val asIntegral: Integral[InternalType]
}
